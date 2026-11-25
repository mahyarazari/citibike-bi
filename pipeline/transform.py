"""Build per-month aggregates, the star schema and live operations tables.

Design notes (see README for the full data dictionary)
- Raw trip-level data never goes to Power BI. Trips are aggregated per month into
  data/agg/YYYYMM/ (committed), so CI runs only process new months.
- Large facts (fact_trips_hourly, fact_station_hourly) are written as one Parquet per
  month: data/clean/fact_trips_hourly_YYYYMM.parquet. Power Query reads the list of
  partitions from meta_partitions and combines them (keeps every file < 50 MB).
- The end station is not in fact_trips_hourly (that grain would exceed size limits);
  origin-destination analysis uses fact_od_monthly (month x start x end x member type).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import polars as pl

from common import get_logger, write_table

log = get_logger("transform")

DAY_PARTS = [(0, 5, "Night"), (6, 9, "AM Peak"), (10, 15, "Midday"), (16, 19, "PM Peak"), (20, 23, "Evening")]


# ====================================================================== per-month aggregates
def aggregate_month(trips: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Aggregate one month of *clean* trips. Returns small tables for data/agg/YYYYMM/."""
    t = trips
    out: dict[str, pl.DataFrame] = {}

    out["trips_hourly"] = (
        t.group_by("date", "hour", "start_station_id", "rideable_type", "member_casual")
        .agg(
            pl.len().cast(pl.Int32).alias("trip_count"),
            pl.col("duration_min").sum().cast(pl.Float32).alias("total_duration_min"),
            pl.col("distance_km").sum().cast(pl.Float32).alias("total_distance_km"),
            pl.col("distance_km").is_not_null().sum().cast(pl.Int32).alias("trips_with_distance"),
            pl.col("flag_unreturned").sum().cast(pl.Int32).alias("unreturned_count"),
        )
    )

    dep = (t.group_by("date", "hour", "start_station_id")
           .agg(pl.len().cast(pl.Int32).alias("departures"))
           .rename({"start_station_id": "station_id"}))
    arr = (t.filter(pl.col("end_station_id").is_not_null())
           .group_by(pl.col("end_date").alias("date"), pl.col("end_hour").alias("hour"), "end_station_id")
           .agg(pl.len().cast(pl.Int32).alias("arrivals"))
           .rename({"end_station_id": "station_id"}))
    month_start, month_end = t["date"].min(), t["date"].max()
    out["station_hourly"] = (
        dep.join(arr, on=["date", "hour", "station_id"], how="full", coalesce=True)
        .filter(pl.col("date").is_between(month_start, month_end))  # arrivals spilling into next month stay out
        .with_columns(pl.col("departures").fill_null(0), pl.col("arrivals").fill_null(0))
        .with_columns((pl.col("arrivals") - pl.col("departures")).alias("net_flow"))
    )

    out["od_monthly"] = (
        t.filter(pl.col("end_station_id").is_not_null())
        .group_by("month_key", "start_station_id", "end_station_id", "member_casual")
        .agg(
            pl.len().cast(pl.Int32).alias("trip_count"),
            pl.col("duration_min").sum().cast(pl.Float32).alias("total_duration_min"),
            pl.col("distance_km").sum().cast(pl.Float32).alias("total_distance_km"),
        )
    )

    # Customer behaviour: duration distribution by segment
    out["customer_profile"] = (
        t.with_columns(
            pl.col("duration_min").cut([5, 10, 15, 20, 30, 45, 60],
                                       labels=["<5", "5-10", "10-15", "15-20", "20-30", "30-45", "45-60", "60+"])
            .cast(pl.String).alias("duration_bucket")
        )
        .group_by("month_key", "member_casual", "rideable_type", "weekday", "hour", "duration_bucket")
        .agg(pl.len().cast(pl.Int32).alias("trip_count"),
             pl.col("duration_min").sum().cast(pl.Float32).alias("total_duration_min"))
    )

    # Conversion target proxy (no rider id exists in the open data!):
    # casual weekday commute-like trips (AM/PM peak, <= 30 min) repeated on the same
    # origin-destination pair >= 3 times in the same ISO week.
    out["casual_repeat_od"] = (
        t.filter((pl.col("member_casual") == "casual") & (pl.col("weekday") <= 5)
                 & pl.col("hour").is_in([6, 7, 8, 9, 16, 17, 18, 19])
                 & (pl.col("duration_min") <= 30) & pl.col("end_station_id").is_not_null())
        .with_columns(pl.col("date").dt.truncate("1w").alias("week_start"))
        .group_by("week_start", "start_station_id", "end_station_id")
        .agg(pl.len().cast(pl.Int32).alias("trips"))
        .filter(pl.col("trips") >= 3)
    )

    out["stations_seen"] = pl.concat([
        t.select(pl.col("start_station_id").alias("short_name"), pl.col("start_station_name").alias("name"),
                 pl.col("start_lat").alias("lat"), pl.col("start_lng").alias("lon")),
        t.filter(pl.col("end_station_id").is_not_null()).select(
            pl.col("end_station_id").alias("short_name"), pl.col("end_station_name").alias("name"),
            pl.col("end_lat").alias("lat"), pl.col("end_lng").alias("lon")),
    ]).group_by("short_name").agg(
        pl.col("name").mode().first(), pl.col("lat").median(), pl.col("lon").median(),
        pl.len().cast(pl.Int32).alias("trip_ends"),
    )
    return out


# ====================================================================== dimensions
def build_dim_date(start: date, end: date) -> pl.DataFrame:
    # US federal holidays observed in NYC (fixed list for the years covered)
    from holidays_us import us_holidays
    d = pl.DataFrame({"date": pl.date_range(start, end, "1d", eager=True)})
    hol = us_holidays(start.year, end.year)
    return d.with_columns(
        (pl.col("date").dt.year() * 10000 + pl.col("date").dt.month() * 100 + pl.col("date").dt.day()).alias("date_key"),
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.quarter().alias("quarter"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.strftime("%b").alias("month_name"),
        (pl.col("date").dt.year() * 100 + pl.col("date").dt.month()).alias("month_key"),
        pl.col("date").dt.strftime("%Y-%m").alias("year_month"),
        pl.col("date").dt.truncate("1w").alias("week_start"),
        pl.col("date").dt.weekday().alias("weekday_num"),
        pl.col("date").dt.strftime("%a").alias("weekday_name"),
        (pl.col("date").dt.weekday() >= 6).alias("is_weekend"),
        pl.col("date").is_in(list(hol.keys())).alias("is_holiday"),
        pl.col("date").replace_strict(hol, default=None, return_dtype=pl.String).alias("holiday_name"),
    )


def build_dim_hour() -> pl.DataFrame:
    rows = []
    for h in range(24):
        part = next(p for a, b, p in DAY_PARTS if a <= h <= b)
        label = f"{(h % 12) or 12} {'AM' if h < 12 else 'PM'}"
        rows.append({"hour": h, "hour_label": label, "day_part": part,
                     "day_part_order": [p for *_, p in DAY_PARTS].index(part) + 1})
    return pl.DataFrame(rows)


def build_dim_station(info_clean: pl.DataFrame, seen: pl.DataFrame, station_geo: pl.DataFrame) -> pl.DataFrame:
    """Active GBFS stations + retired stations seen in trips (is_active = False)."""
    active = info_clean.select(
        "short_name", "name", "lat", "lon", "capacity", "region_id", "region_name", "station_id",
    ).with_columns(pl.lit(True).alias("is_active"))
    retired = (seen.filter(~pl.col("short_name").is_in(active["short_name"].implode()))
               .filter(pl.col("lat").is_not_null())
               .select("short_name", "name", "lat", "lon")
               .with_columns(pl.lit(None, pl.Int64).alias("capacity"), pl.lit(None, pl.String).alias("region_id"),
                             pl.lit("Retired / not in GBFS").alias("region_name"), pl.lit(None, pl.String).alias("station_id"),
                             pl.lit(False).alias("is_active")))
    dim = pl.concat([active, retired], how="vertical_relaxed")
    dim = dim.join(station_geo, on="short_name", how="left")
    return dim.sort("short_name").with_row_index("station_key", offset=1).with_columns(
        pl.col("station_key").cast(pl.Int32),
        pl.col("nta_code").fill_null("OUTSIDE"),
        pl.col("nta_name").fill_null("Outside NYC NTAs"),
        pl.col("borough").fill_null("Outside NYC"),
    )


# ====================================================================== live operations
def classify(fill: pl.Expr, bikes: pl.Expr, docks: pl.Expr, low: float, high: float) -> pl.Expr:
    return (pl.when(bikes == 0).then(pl.lit("Empty"))
            .when(docks == 0).then(pl.lit("Full"))
            .when(fill < low).then(pl.lit("Low"))
            .when(fill > high).then(pl.lit("High"))
            .otherwise(pl.lit("Balanced")))


def update_status_history(history: pl.DataFrame | None, snap: pl.DataFrame, retention_days: int) -> pl.DataFrame:
    """Append the new snapshot, de-duplicate, keep a rolling window."""
    cols = ["snapshot_utc", "station_id", "num_bikes_available", "num_ebikes_available", "num_bikes_disabled",
            "num_docks_available", "num_docks_disabled", "is_renting", "is_returning", "last_reported_utc"]
    snap = snap.select(cols)
    h = snap if history is None or history.height == 0 else pl.concat([history.select(cols), snap], how="vertical_relaxed")
    cutoff = snap["snapshot_utc"].max() - timedelta(days=retention_days)
    return h.unique(subset=["snapshot_utc", "station_id"], keep="last").filter(pl.col("snapshot_utc") >= cutoff).sort("snapshot_utc", "station_id")


def live_station_metrics(history: pl.DataFrame, dim_station: pl.DataFrame, hist_hourly: pl.DataFrame | None,
                         cfg: dict) -> pl.DataFrame:
    """Per-station live metrics for the latest snapshot.

    net_flow_per_hour: least-squares slope of bikes over the last N snapshots (bikes/hour).
    When fewer than 2 snapshots exist (first run), the historical average net flow for
    this weekday x hour (from trips) is used instead; source is recorded in flow_source.
    """
    lc = cfg["live"]
    latest = history["snapshot_utc"].max()
    snaps = history.select("snapshot_utc").unique().sort("snapshot_utc").tail(lc["trend_snapshots"])["snapshot_utc"]
    recent = history.filter(pl.col("snapshot_utc").is_in(snaps.implode()))

    slope = (recent.with_columns(((pl.col("snapshot_utc") - latest).dt.total_seconds() / 3600).alias("t_h"))
             .group_by("station_id")
             .agg(pl.len().alias("n"),
                  pl.cov("t_h", "num_bikes_available").alias("cov"), pl.col("t_h").var().alias("var"))
             .with_columns(pl.when((pl.col("n") >= 2) & (pl.col("var") > 0)).then(pl.col("cov") / pl.col("var"))
                           .otherwise(None).alias("trend_flow"))
             .select("station_id", "trend_flow"))

    cur = history.filter(pl.col("snapshot_utc") == latest)
    st = dim_station.filter(pl.col("is_active")).select("station_id", "station_key", "short_name", "name", "capacity",
                                                        "lat", "lon", "nta_code", "borough", "region_name")
    df = cur.join(st, on="station_id", how="inner").join(slope, on="station_id", how="left")

    # Historical demand at this weekday-hour (NYC local)
    now_local = latest.astimezone(__import__("zoneinfo").ZoneInfo(cfg["cleaning"]["timezone"]))
    if hist_hourly is not None and hist_hourly.height:
        hh = (hist_hourly.filter((pl.col("weekday") == now_local.isoweekday()) & (pl.col("hour") == now_local.hour))
              .select(pl.col("station_id").alias("short_name"), "avg_departures", "avg_arrivals", "avg_net_flow"))
        df = df.join(hh, on="short_name", how="left")
    else:
        df = df.with_columns(pl.lit(None, pl.Float64).alias(c) for c in ("avg_departures", "avg_arrivals", "avg_net_flow"))

    p95 = df["avg_departures"].fill_null(0).quantile(0.95) or 1.0
    bikes, docks = pl.col("num_bikes_available"), pl.col("num_docks_available")
    cap = pl.col("capacity")
    rate = pl.coalesce(pl.col("trend_flow"), pl.col("avg_net_flow"))  # bikes per hour (+ = filling)

    df = df.with_columns(
        pl.when(cap > 0).then(bikes / cap).otherwise(None).alias("fill_ratio"),
        rate.alias("net_flow_per_hour"),
        pl.when(pl.col("trend_flow").is_not_null()).then(pl.lit("snapshot trend"))
        .when(pl.col("avg_net_flow").is_not_null()).then(pl.lit("historical avg"))
        .otherwise(pl.lit("none")).alias("flow_source"),
    ).with_columns(
        classify(pl.col("fill_ratio"), bikes, docks, lc["low_threshold"], lc["high_threshold"]).alias("status_class"),
        pl.when(bikes == 0).then(0.0).when(rate < 0).then(bikes / (-rate) * 60).otherwise(None).alias("est_minutes_to_empty"),
        pl.when(docks == 0).then(0.0).when(rate > 0).then(docks / rate * 60).otherwise(None).alias("est_minutes_to_full"),
    ).with_columns(
        pl.min_horizontal("est_minutes_to_empty", "est_minutes_to_full").alias("_mins"),
        (pl.max_horizontal(pl.col("avg_departures").fill_null(0), pl.col("avg_arrivals").fill_null(0)) / p95)
        .clip(0, 1).alias("demand_index"),
    ).with_columns(
        # urgency: 1 when empty/full now, decays with time-to-event (half-life ~ 42 min)
        pl.when(pl.col("_mins").is_null()).then(0.0).otherwise((-pl.col("_mins") / 60).exp()).alias("urgency"),
    ).with_columns(
        (100 * pl.col("urgency") * (0.5 + 0.5 * pl.col("demand_index"))).round(1).alias("rebalance_priority_score"),
        # bikes to add (+) or remove (-) to bring the station back to 50 % fill
        pl.when(cap > 0).then((cap * 0.5 - bikes).round(0)).otherwise(None).cast(pl.Int32).alias("bikes_to_target"),
    ).drop("_mins")
    return df


def build_rebalance_list(live: pl.DataFrame, n: int) -> pl.DataFrame:
    r = (live.filter(pl.col("rebalance_priority_score") > 0)
         .sort(["rebalance_priority_score", pl.col("bikes_to_target").abs()], descending=True).head(n)
         .with_row_index("priority_rank", offset=1)
         .with_columns(
             pl.when(pl.col("bikes_to_target") > 0)
             .then(pl.format("Deliver {} bikes", pl.col("bikes_to_target")))
             .otherwise(pl.format("Pick up {} bikes", -pl.col("bikes_to_target"))).alias("action")))
    return r.select("priority_rank", "snapshot_utc", "station_key", "short_name", "name", "nta_code", "borough",
                    "status_class", "num_bikes_available", "num_docks_available", "capacity", "fill_ratio",
                    "net_flow_per_hour", "flow_source", "est_minutes_to_empty", "est_minutes_to_full",
                    "demand_index", "rebalance_priority_score", "bikes_to_target", "action")


def neighborhood_live(history: pl.DataFrame, dim_station: pl.DataFrame, dim_nbhd: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    """Per NTA: current bikes/docks, empty/full counts, % balanced, bikes per 1k residents,
    change vs. the snapshot closest to 1 hour ago."""
    lc = cfg["live"]
    st = dim_station.filter(pl.col("is_active")).select("station_id", "nta_code", "capacity")

    def agg(snap_time):
        s = history.filter(pl.col("snapshot_utc") == snap_time).join(st, on="station_id")
        s = s.with_columns(classify(pl.when(pl.col("capacity") > 0).then(pl.col("num_bikes_available") / pl.col("capacity")),
                                    pl.col("num_bikes_available"), pl.col("num_docks_available"),
                                    lc["low_threshold"], lc["high_threshold"]).alias("status_class"))
        return s.group_by("nta_code").agg(
            pl.len().alias("stations_reporting"),
            pl.col("num_bikes_available").sum().alias("bikes_available"),
            pl.col("num_ebikes_available").sum().alias("ebikes_available"),
            pl.col("num_docks_available").sum().alias("docks_available"),
            (pl.col("status_class") == "Empty").sum().alias("empty_stations"),
            (pl.col("status_class") == "Full").sum().alias("full_stations"),
            (pl.col("status_class") == "Balanced").sum().alias("balanced_stations"),
        )

    times = history.select("snapshot_utc").unique().sort("snapshot_utc")["snapshot_utc"]
    latest = times.max()
    target = latest - timedelta(hours=1)
    prev_candidates = [t for t in times if t <= latest - timedelta(minutes=30)]
    prev = min(prev_candidates, key=lambda t: abs((t - target).total_seconds())) if prev_candidates else None

    cur = agg(latest).with_columns(pl.lit(latest).alias("snapshot_utc"))
    if prev is not None:
        p = agg(prev).select("nta_code", pl.col("bikes_available").alias("bikes_1h_ago"),
                             (pl.col("balanced_stations") / pl.col("stations_reporting")).alias("pct_balanced_1h_ago"))
        cur = cur.join(p, on="nta_code", how="left").with_columns(pl.lit(prev).alias("compare_snapshot_utc"))
    else:
        cur = cur.with_columns(pl.lit(None, pl.Int64).alias("bikes_1h_ago"), pl.lit(None, pl.Float64).alias("pct_balanced_1h_ago"),
                               pl.lit(None, pl.Datetime("us", "UTC")).alias("compare_snapshot_utc"))
    cur = cur.join(dim_nbhd.select("nta_code", "population"), on="nta_code", how="left").with_columns(
        (pl.col("balanced_stations") / pl.col("stations_reporting")).alias("pct_balanced"),
        pl.when(pl.col("population") > 0).then(pl.col("bikes_available") / pl.col("population") * 1000)
        .otherwise(None).alias("bikes_per_1k_residents"),
    ).with_columns(
        (pl.col("bikes_available") - pl.col("bikes_1h_ago")).alias("bikes_change_1h"),
        (pl.col("pct_balanced") - pl.col("pct_balanced_1h_ago")).alias("pct_balanced_change_1h"),
    ).drop("population")
    return cur


def historical_station_profile(station_hourly: pl.DataFrame) -> pl.DataFrame:
    """Average departures/arrivals/net flow per station x weekday x hour (from trips)."""
    days = (station_hourly.select("date").unique()
            .with_columns(pl.col("date").dt.weekday().alias("weekday"))
            .group_by("weekday").agg(pl.len().alias("n_days")))
    return (station_hourly.with_columns(pl.col("date").dt.weekday().alias("weekday"))
            .group_by("station_id", "weekday", "hour")
            .agg(pl.col("departures").sum(), pl.col("arrivals").sum(), pl.col("net_flow").sum())
            .join(days, on="weekday")
            .with_columns((pl.col("departures") / pl.col("n_days")).alias("avg_departures"),
                          (pl.col("arrivals") / pl.col("n_days")).alias("avg_arrivals"),
                          (pl.col("net_flow") / pl.col("n_days")).alias("avg_net_flow"))
            .select("station_id", "weekday", "hour", "avg_departures", "avg_arrivals", "avg_net_flow"))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
