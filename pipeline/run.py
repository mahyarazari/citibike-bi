"""Orchestrates the whole pipeline. Idempotent: re-running produces the same outputs.

Usage
  python pipeline/run.py                 # full: live snapshot + incremental trips + model
  python pipeline/run.py --mode live     # 30-min job: GBFS snapshot + live tables only
  python pipeline/run.py --mode trips    # monthly job: trips + model (+ live snapshot)
  python pipeline/run.py --months 3 --force
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))

import clean as C          # noqa: E402
import extract_gbfs        # noqa: E402
import extract_trips       # noqa: E402
import geo                 # noqa: E402
import transform as T      # noqa: E402
import validate as V       # noqa: E402
import weather             # noqa: E402
from common import get_logger, load_config, new_run_id, read_json, write_json, write_table  # noqa: E402

log = get_logger("run")


class Run:
    def __init__(self, cfg: dict, mode: str):
        self.cfg, self.mode = cfg, mode
        self.run_id = new_run_id()
        self.run_time = datetime.now(timezone.utc).replace(microsecond=0)
        self.dq: list[dict] = []
        self.written: list[dict] = []
        self.clean = cfg["paths"]["clean_dir"]
        self.agg = cfg["paths"]["agg_dir"]

    # ------------------------------------------------------------------ helpers
    def log_dq(self, dqlog: C.DQLog, scope: str):
        for e in dqlog.entries:
            self.dq.append({"run_id": self.run_id, "run_time_utc": self.run_time, "scope": scope, **e})

    def out(self, df: pl.DataFrame, name: str, csv: bool = True):
        info = write_table(df, name, self.cfg, csv=csv)
        self.written.append(info)
        log.info("  wrote %-32s %9s rows  parquet %.2f MB  csv %s", name, f"{info['rows']:,}",
                 info["parquet_mb"], f"{info['csv_mb']:.2f} MB" if info["csv_mb"] is not None else "skipped (>limit)")

    def read_clean(self, name: str) -> pl.DataFrame | None:
        p = self.clean / f"{name}.parquet"
        return pl.read_parquet(p) if p.exists() else None

    # ------------------------------------------------------------------ steps
    def stations(self):
        raw = extract_gbfs.extract(self.cfg)
        V.check(V.StationStatus, raw["station_status"])
        info, dq_i = C.clean_station_info(raw["station_info"], raw["system_regions"], self.cfg)
        V.check(V.StationInfo, info)
        status, dq_s = C.clean_status(raw["station_status"], self.cfg)
        self.log_dq(dq_i, "live")
        self.log_dq(dq_s, "live")
        self.info, self.status, self.regions = info, status, raw["system_regions"]

    def trips(self, months: int | None, force: bool):
        manifest = extract_trips.run(self.cfg, force=force, months=months)
        known = set(self.info["short_name"].to_list())
        latest = max(manifest["window"]) if manifest["window"] else None
        for m, entry in sorted(manifest["months"].items()):
            if entry.get("status") == "processed" and (self.agg / m / "_SUCCESS").exists() and not force:
                continue
            t0 = time.time()
            raw = pl.read_parquet(self.cfg["paths"]["work_dir"] / f"trips_raw_{m}.parquet")
            V.check(V.RawTrips, raw, sample=200_000)
            if m == latest:
                self.write_sample(raw)
            clean, dqlog, unmatched = C.clean_trips(raw.drop("source_file"), self.cfg, known)
            del raw
            V.check(V.CleanTrips, clean, sample=200_000)
            aggs = T.aggregate_month(clean)
            del clean
            d = self.agg / m
            shutil.rmtree(d, ignore_errors=True)
            d.mkdir(parents=True)
            for name, df in aggs.items():
                df.write_parquet(d / f"{name}.parquet", compression="zstd")
            unmatched.write_parquet(d / "unmatched.parquet")
            dq = pl.DataFrame([{"month_key": int(m), **e} for e in dqlog.entries])
            dq.write_parquet(d / "dq.parquet")
            (d / "_SUCCESS").write_text(self.run_id)
            self.log_dq(dqlog, f"trips {m}")
            entry.update(status="processed", processed_run_id=self.run_id, rows_clean=int(dq["rows_out"].min()))
            write_json(self.cfg["paths"]["manifest"], manifest)
            log.info("%s cleaned + aggregated in %.0fs", m, time.time() - t0)
        self.manifest = manifest

    def write_sample(self, raw: pl.DataFrame):
        """Raw (uncleaned) trips of the last N days of the latest month - used by the
        Power Query cleaning demo so cleaning steps are visible inside Power BI."""
        tc = self.cfg["trips"]
        s = raw.with_columns(pl.col("started_at").str.slice(0, 10).alias("_d"))
        days = sorted(s["_d"].drop_nulls().unique().to_list())[-tc["sample_days"]:]
        s = s.filter(pl.col("_d").is_in(days)).drop("_d", "source_file")
        if s.height > tc["sample_max_rows"]:
            s = s.sample(n=tc["sample_max_rows"], seed=7)
        p = self.cfg["paths"]["sample_dir"] / "trips_raw_sample.csv"
        s.write_csv(p)
        log.info("Raw sample for Power Query demo: %s rows (%s..%s) -> %s, %.1f MB",
                 f"{s.height:,}", days[0], days[-1], p.name, p.stat().st_size / 1e6)

    def model(self):
        months = sorted(p.name for p in self.agg.iterdir() if (p / "_SUCCESS").exists())
        if not months:
            raise RuntimeError("No processed trip months found in data/agg - run with --mode trips first")
        log.info("Building star schema from months: %s", ", ".join(months))

        def cat(name):
            return pl.concat([pl.read_parquet(self.agg / m / f"{name}.parquet") for m in months], how="vertical_relaxed")

        # --- geography + stations
        nbhd = geo.load_neighborhoods(self.cfg)
        seen = cat("stations_seen").group_by("short_name").agg(
            pl.col("name").sort_by("trip_ends").last(), pl.col("lat").median(), pl.col("lon").median(),
            pl.col("trip_ends").sum())
        candidates = pl.concat([self.info.select("short_name", "name", "lat", "lon"),
                                seen.filter(~pl.col("short_name").is_in(self.info["short_name"].implode()))
                                    .select("short_name", "name", "lat", "lon")], how="vertical_relaxed")
        mapping, outside = geo.join_stations(candidates, nbhd)
        dim_station = T.build_dim_station(self.info, seen, mapping)
        dim_nbhd = geo.build_dim_neighborhood(nbhd, dim_station, self.cfg)
        self.dim_station, self.dim_nbhd = dim_station, dim_nbhd
        self.map_sizes = geo.export_map(nbhd, dim_nbhd, self.cfg)

        key = dim_station.select("short_name", "station_key")
        # --- facts partitioned by month
        parts = []
        for m in months:
            th = (pl.read_parquet(self.agg / m / "trips_hourly.parquet")
                  .join(key.rename({"short_name": "start_station_id", "station_key": "start_station_key"}),
                        on="start_station_id", how="left")
                  .with_columns((pl.col("date").dt.year() * 10000 + pl.col("date").dt.month() * 100
                                 + pl.col("date").dt.day()).alias("date_key")))
            self.out(th, f"fact_trips_hourly_{m}", csv=False)
            sh = (pl.read_parquet(self.agg / m / "station_hourly.parquet")
                  .join(key.rename({"short_name": "station_id"}), on="station_id", how="left")
                  .with_columns((pl.col("date").dt.year() * 10000 + pl.col("date").dt.month() * 100
                                 + pl.col("date").dt.day()).alias("date_key")))
            self.out(sh, f"fact_station_hourly_{m}", csv=False)
            parts += [{"table": "fact_trips_hourly", "month_key": int(m), "file": f"fact_trips_hourly_{m}.parquet"},
                      {"table": "fact_station_hourly", "month_key": int(m), "file": f"fact_station_hourly_{m}.parquet"}]
        # remove partitions that fell out of the window
        for p in self.clean.glob("fact_*_hourly_*.parquet"):
            if p.stem.rsplit("_", 1)[-1] not in months:
                p.unlink()
        self.out(pl.DataFrame(parts), "meta_partitions")
        # Single combined files for Power BI when they fit the 50 MB limit (true for 1-2 months).
        # Power Query then reads one static URL per table (no dynamic-URL / formula-firewall issues).
        for t in ("fact_trips_hourly", "fact_station_hourly"):
            combined = pl.concat([pl.read_parquet(self.clean / f"{t}_{m}.parquet") for m in months], how="vertical_relaxed")
            try:
                self.out(combined, t, csv=False)
            except ValueError as e:
                (self.clean / f"{t}.parquet").unlink(missing_ok=True)
                log.warning("%s too large for one file (%s) - Power BI must combine partitions", t, e)

        od = (cat("od_monthly")
              .join(key.rename({"short_name": "start_station_id", "station_key": "start_station_key"}), on="start_station_id", how="left")
              .join(key.rename({"short_name": "end_station_id", "station_key": "end_station_key"}), on="end_station_id", how="left"))
        self.out(od.drop("start_station_id", "end_station_id"), "fact_od_monthly", csv=False)
        # top routes with names/coordinates for the routes table + flow map (small, Power BI friendly)
        st = dim_station.select("station_key", "name", "lat", "lon", "nta_name", "borough")
        days = max(1, pl.concat([pl.read_parquet(self.agg / m / "station_hourly.parquet", columns=["date"])
                                 for m in months])["date"].n_unique())
        top = (od.filter(pl.col("start_station_key") != pl.col("end_station_key"))
               .group_by("start_station_key", "end_station_key")
               .agg(pl.col("trip_count").sum().alias("trips"),
                    pl.col("trip_count").filter(pl.col("member_casual") == "member").sum().alias("member_trips"),
                    pl.col("total_duration_min").sum(), pl.col("total_distance_km").sum())
               .sort("trips", descending=True).head(500)
               .with_row_index("route_rank", offset=1)
               .join(st.rename({c: f"start_{c}" for c in st.columns}), on="start_station_key", how="left")
               .join(st.rename({c: f"end_{c}" for c in st.columns}), on="end_station_key", how="left")
               .with_columns(
                   pl.format("{} → {}", "start_name", "end_name").alias("route"),
                   (pl.col("trips") / days).alias("trips_per_day"),
                   (pl.col("total_duration_min") / pl.col("trips")).alias("avg_duration_min"),
                   (pl.col("total_distance_km") / pl.col("trips")).alias("avg_distance_km"),
                   (pl.col("member_trips") / pl.col("trips")).alias("member_share")))
        self.out(top, "fact_top_routes")
        self.out(cat("customer_profile"), "fact_customer_profile")
        self.out(cat("casual_repeat_od")
                 .join(key.rename({"short_name": "start_station_id", "station_key": "start_station_key"}), on="start_station_id", how="left")
                 .join(key.rename({"short_name": "end_station_id", "station_key": "end_station_key"}), on="end_station_id", how="left"),
                 "fact_casual_repeat_od")
        um = cat("unmatched").group_by("station_id").agg(pl.col("station_name").first(), pl.col("trips").sum()).sort("trips", descending=True)
        self.out(um, "fact_unmatched_stations")
        self.out(outside, "fact_stations_outside_nta")
        tdq = pl.concat([pl.read_parquet(self.agg / m / "dq.parquet") for m in months], how="vertical_relaxed")
        self.out(tdq, "fact_trip_cleaning")

        # --- historical profile (for live priority + demand page)
        station_hourly_all = pl.concat([pl.read_parquet(self.agg / m / "station_hourly.parquet") for m in months])
        self.profile = T.historical_station_profile(station_hourly_all)
        self.out(self.profile.join(key.rename({"short_name": "station_id"}), on="station_id", how="left"),
                 "fact_station_profile", csv=False)

        # --- dimensions
        first = station_hourly_all["date"].min()
        last = max(station_hourly_all["date"].max(), date.today())
        first_dim = date(first.year - 1, first.month, 1)  # prior year for YoY measures
        self.out(T.build_dim_date(first_dim, date(last.year, 12, 31)), "dim_date")
        self.out(T.build_dim_hour(), "dim_hour")
        self.out(dim_station.drop("station_id"), "dim_station")
        self.out(self.regions, "dim_region")
        self.out(dim_nbhd, "dim_neighborhood")
        try:
            w = weather.fetch(first, station_hourly_all["date"].max())
            # city-wide departures per hour and the "normal" level for that weekday-type x hour,
            # so weather impact is measured against the same hour (removes time-of-day bias)
            hourly = (station_hourly_all.group_by("date", "hour").agg(pl.col("departures").sum().alias("trips"))
                      .with_columns((pl.col("date").dt.weekday() >= 6).alias("is_weekend")))
            hourly = hourly.with_columns(pl.col("trips").mean().over("is_weekend", "hour").alias("expected_trips"))
            w = (w.join(hourly.select("date", pl.col("hour").cast(pl.Int8), "trips", "expected_trips"),
                        on=["date", "hour"], how="left")
                 .with_columns((pl.col("trips") / pl.col("expected_trips")).alias("trips_vs_normal")))
            self.out(w, "fact_weather_hourly")
        except Exception as e:  # optional enrichment must not break the run
            log.warning("Weather enrichment skipped: %s", e)
        self.trip_window = (first, station_hourly_all["date"].max())

    def load_model_dims(self):
        """Live mode: reuse dimensions built by the last full run."""
        ds = self.read_clean("dim_station")
        dn = self.read_clean("dim_neighborhood")
        if ds is None or dn is None:
            raise RuntimeError("Dimensions missing - run a full pipeline first")
        # refresh station_id (GBFS uuid) for active stations from today's feed
        self.dim_station = ds.join(self.info.select("short_name", "station_id"), on="short_name", how="left")
        self.dim_nbhd = dn
        prof = self.read_clean("fact_station_profile")
        self.profile = prof.drop("station_key") if prof is not None else None

    def live(self):
        snap = self.status.join(self.dim_station.filter(pl.col("is_active")).select("station_id"), on="station_id")
        hist = T.update_status_history(self.read_clean("fact_station_status"), snap,
                                       self.cfg["live"]["snapshot_retention_days"])
        keys = self.dim_station.filter(pl.col("is_active")).select("station_id", "station_key")
        self.out(hist.join(keys, on="station_id", how="left"), "fact_station_status", csv=False)
        live = T.live_station_metrics(hist, self.dim_station, self.profile, self.cfg)
        V.check(V.LiveStation, live)
        self.out(live.drop("station_id"), "fact_station_live")
        self.out(T.build_rebalance_list(live, self.cfg["live"]["rebalance_list_size"]), "fact_rebalance_list")
        self.out(T.neighborhood_live(hist, self.dim_station, self.dim_nbhd, self.cfg), "fact_neighborhood_live")
        self.snapshot_utc = hist["snapshot_utc"].max()
        self.snapshots_kept = hist["snapshot_utc"].n_unique()

    def finish(self, status: str = "success", error: str | None = None):
        prev = self.read_clean("fact_data_quality")
        cur = pl.DataFrame(self.dq) if self.dq else None
        if cur is not None:
            cur = cur.with_columns(pl.col("run_time_utc").cast(pl.Datetime("us", "UTC")))
            dq = cur if prev is None else pl.concat([prev, cur], how="diagonal_relaxed")
            self.out(dq.tail(20000), "fact_data_quality")

        # run log (one row per run) for the Data Quality page
        runs = self.read_clean("fact_pipeline_runs")
        row = pl.DataFrame([{"run_id": self.run_id, "run_time_utc": self.run_time, "mode": self.mode,
                             "status": status, "error": error,
                             "duration_s": round((datetime.now(timezone.utc) - self.run_time).total_seconds(), 1)}])
        row = row.with_columns(pl.col("run_time_utc").cast(pl.Datetime("us", "UTC")))
        runs = row if runs is None else pl.concat([runs, row], how="diagonal_relaxed").tail(5000)
        self.out(runs, "fact_pipeline_runs")
        if status != "success":
            return

        meta = read_json(self.clean / "meta_refresh.json", {})
        meta.update({"last_successful_run_utc": self.run_time.isoformat(), "last_run_id": self.run_id,
                     "last_mode": self.mode})
        if hasattr(self, "snapshot_utc"):
            meta.update(last_live_snapshot_utc=self.snapshot_utc.isoformat(), snapshots_kept=self.snapshots_kept)
        if hasattr(self, "trip_window"):
            meta.update(trip_window_start=str(self.trip_window[0]), trip_window_end=str(self.trip_window[1]),
                        last_trip_refresh_utc=self.run_time.isoformat())
        write_json(self.clean / "meta_refresh.json", meta)
        self.out(pl.DataFrame([{k: str(v) for k, v in meta.items()}]), "meta_refresh")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Citi Bike NYC BI pipeline")
    ap.add_argument("--mode", choices=["full", "live", "trips"], default="full")
    ap.add_argument("--months", type=int, help="override trips.months_to_keep")
    ap.add_argument("--force", action="store_true", help="re-process months even if already done")
    a = ap.parse_args(argv)

    cfg = load_config()
    r = Run(cfg, a.mode)
    log.info("Run %s mode=%s", r.run_id, a.mode)
    try:
        r.stations()
        if a.mode in ("full", "trips"):
            r.trips(a.months, a.force)
            r.model()
        else:
            r.load_model_dims()
        r.live()
        r.finish()
    except Exception as e:
        log.exception("Run failed")
        r.finish("failed", f"{type(e).__name__}: {e}")
        raise
    log.info("Run %s finished OK - %d tables written", r.run_id, len(r.written))
    return r


if __name__ == "__main__":
    main()
