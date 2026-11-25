"""Cleaning rules for trips and live station status.

Every rule records how many rows it affected; run.py writes these to fact_data_quality.
Rules are pure functions on polars frames so they are unit-testable (tests/).

Rule types
- REMOVE : rows are dropped from the clean output
- FLAG   : rows are kept, a boolean flag column is set (and the value may be nulled)
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

import polars as pl

TS_FORMAT = "%Y-%m-%d %H:%M:%S%.f"
EARTH_RADIUS_KM = 6371.0088


@dataclass
class DQLog:
    """Collects one entry per cleaning rule."""
    entity: str
    entries: list[dict] = field(default_factory=list)

    def add(self, rule: str, action: str, affected: int, rows_in: int, rows_out: int, note: str = ""):
        self.entries.append({
            "entity": self.entity, "rule": rule, "action": action,
            "rows_affected": int(affected), "rows_in": int(rows_in), "rows_out": int(rows_out), "note": note,
        })


# ---------------------------------------------------------------- helpers
def normalize_name(col: str) -> pl.Expr:
    """Trim, collapse inner whitespace, consistent Title-ish casing kept as source but trimmed.

    Citi Bike names are already mixed case ("W 21 St & 6 Ave"); we only trim and
    collapse whitespace and unify '&' spacing so the same station never appears twice.
    """
    return (
        pl.col(col).str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.replace_all(r"\s*&\s*", " & ")
    )


def haversine_km(lat1: pl.Expr, lon1: pl.Expr, lat2: pl.Expr, lon2: pl.Expr) -> pl.Expr:
    r = math.pi / 180
    dlat = (lat2 - lat1) * r
    dlon = (lon2 - lon1) * r
    a = (dlat / 2).sin() ** 2 + (lat1 * r).cos() * (lat2 * r).cos() * (dlon / 2).sin() ** 2
    return 2 * EARTH_RADIUS_KM * a.sqrt().arcsin()


def excluded_station_expr(col: str, keywords: list[str]) -> pl.Expr:
    # whole words only, so e.g. "Slab City" or "Contest St" are not excluded by "lab"/"test"
    pattern = r"(?i)\b(" + "|".join(k.replace(" ", r"\s+") for k in keywords) + r")\b"
    return pl.col(col).str.contains(pattern).fill_null(False)


# ---------------------------------------------------------------- trips
def clean_trips(df: pl.DataFrame, cfg: dict, known_short_names: set[str] | None = None
                ) -> tuple[pl.DataFrame, DQLog, pl.DataFrame]:
    """Apply all trip cleaning rules.

    Input: raw trips with every column as string (IDs must stay strings).
    Returns (clean_df, dq_log, unmatched_station_ids_df).
    """
    c = cfg["cleaning"]
    dq = DQLog("trips")
    n0 = df.height

    # R1 duplicates ------------------------------------------------------
    df = df.unique(subset=["ride_id"], keep="first", maintain_order=True)
    dq.add("R01 duplicate ride_id", "REMOVE", n0 - df.height, n0, df.height)

    # R2 parse timestamps (source is local NYC wall-clock time without offset) ----
    n = df.height
    df = df.with_columns(
        pl.col("started_at").str.to_datetime(TS_FORMAT, strict=False)
        .dt.replace_time_zone(c["timezone"], ambiguous="earliest", non_existent="null").alias("started_at"),
        pl.col("ended_at").str.to_datetime(TS_FORMAT, strict=False)
        .dt.replace_time_zone(c["timezone"], ambiguous="latest", non_existent="null").alias("ended_at"),
    ).filter(pl.col("started_at").is_not_null() & pl.col("ended_at").is_not_null())
    dq.add("R02 unparseable timestamp", "REMOVE", n - df.height, n, df.height)

    # R3 duration ---------------------------------------------------------
    df = df.with_columns(
        ((pl.col("ended_at") - pl.col("started_at")).dt.total_seconds() / 60).alias("duration_min")
    )
    n = df.height
    short = df.filter(pl.col("duration_min") < c["min_duration_min"]).height
    long_ = df.filter(pl.col("duration_min") > c["max_duration_min"]).height
    df = df.filter(pl.col("duration_min").is_between(c["min_duration_min"], c["max_duration_min"]))
    dq.add("R03 duration < 1 min (false start)", "REMOVE", short, n, n - short)
    dq.add("R04 duration > 24 h (lost/unreturned)", "REMOVE", long_, n - short, df.height)

    # R5 station names normalized + empty strings -> null --------------------
    df = df.with_columns(
        normalize_name("start_station_name").alias("start_station_name"),
        normalize_name("end_station_name").alias("end_station_name"),
        pl.col("start_station_id").str.strip_chars().replace("", None).alias("start_station_id"),
        pl.col("end_station_id").str.strip_chars().replace("", None).alias("end_station_id"),
    )
    # malformed ids with a trailing underscore ("6517.08_") are the same station as "6517.08"
    fixed = df.filter(pl.col("start_station_id").str.ends_with("_") | pl.col("end_station_id").str.ends_with("_")).height
    df = df.with_columns(
        pl.col("start_station_id").str.strip_chars_end("_"),
        pl.col("end_station_id").str.strip_chars_end("_"),
    )
    dq.add("R05a station id with trailing '_' repaired", "FIX", fixed, df.height, df.height)
    df = df.with_columns(
        pl.when(pl.col("start_station_name") == "").then(None).otherwise(pl.col("start_station_name")).alias("start_station_name"),
        pl.when(pl.col("end_station_name") == "").then(None).otherwise(pl.col("end_station_name")).alias("end_station_name"),
    )

    # R6 missing start station: cannot attribute demand -> remove ----------------
    n = df.height
    df = df.filter(pl.col("start_station_id").is_not_null())
    dq.add("R05 missing start station", "REMOVE", n - df.height, n, df.height)

    # R7 missing end station -> flag unreturned (kept) -------------------------
    df = df.with_columns(pl.col("end_station_id").is_null().alias("flag_unreturned"))
    dq.add("R06 missing end station (unreturned)", "FLAG",
           df["flag_unreturned"].sum(), df.height, df.height)

    # R8 test / depot / lab stations -------------------------------------------
    n = df.height
    kw = c["excluded_station_keywords"]
    id_pat = "|".join(c.get("excluded_station_id_patterns", [])) or "^$"
    df = df.filter(
        ~excluded_station_expr("start_station_name", kw) & ~excluded_station_expr("end_station_name", kw)
        & ~pl.col("start_station_id").str.contains(id_pat).fill_null(False)
        & ~pl.col("end_station_id").str.contains(id_pat).fill_null(False)
    )
    dq.add("R07 test/depot/lab/internal or out-of-scope station", "REMOVE", n - df.height, n, df.height,
           "names: " + ", ".join(kw) + " | ids: " + id_pat)

    # R9 coordinates -> float, outside NYC bbox -> null + flag ------------------
    b = c["nyc_bbox"]
    df = df.with_columns(pl.col(x).cast(pl.Float64, strict=False) for x in ("start_lat", "start_lng", "end_lat", "end_lng"))

    def in_bbox(lat, lng):
        return pl.col(lat).is_between(b["lat_min"], b["lat_max"]) & pl.col(lng).is_between(b["lng_min"], b["lng_max"])

    df = df.with_columns(
        (pl.col("start_lat").is_not_null() & ~in_bbox("start_lat", "start_lng")).alias("_bad_s"),
        (pl.col("end_lat").is_not_null() & ~in_bbox("end_lat", "end_lng")).alias("_bad_e"),
    ).with_columns(
        (pl.col("_bad_s") | pl.col("_bad_e")).alias("flag_bad_coords"),
        *[pl.when(pl.col("_bad_s")).then(None).otherwise(pl.col(x)).alias(x) for x in ("start_lat", "start_lng")],
        *[pl.when(pl.col("_bad_e")).then(None).otherwise(pl.col(x)).alias(x) for x in ("end_lat", "end_lng")],
    ).drop("_bad_s", "_bad_e")
    dq.add("R08 coordinates outside NYC bbox", "FLAG", df["flag_bad_coords"].sum(), df.height, df.height,
           "values set to null")

    # R10 haversine distance + implausible speed (classic bikes) ----------------
    df = df.with_columns(
        haversine_km(pl.col("start_lat"), pl.col("start_lng"), pl.col("end_lat"), pl.col("end_lng")).alias("distance_km")
    ).with_columns(
        (pl.col("distance_km") / (pl.col("duration_min") / 60)).alias("speed_kmh")
    ).with_columns(
        ((pl.col("rideable_type") == "classic_bike") & (pl.col("speed_kmh") > c["max_speed_kmh_classic"]))
        .fill_null(False).alias("flag_implausible_speed")
    ).with_columns(
        pl.when(pl.col("flag_implausible_speed")).then(None).otherwise(pl.col("distance_km")).alias("distance_km")
    )
    dq.add("R09 implausible speed > 40 km/h (classic)", "FLAG", df["flag_implausible_speed"].sum(),
           df.height, df.height, "distance set to null")

    # R11 station mapping to GBFS short_name -----------------------------------
    unmatched = pl.DataFrame({"station_id": [], "station_name": [], "trips": []},
                             schema={"station_id": pl.String, "station_name": pl.String, "trips": pl.UInt32})
    if known_short_names is not None:
        known = pl.Series(list(known_short_names), dtype=pl.String)
        starts = df.select(pl.col("start_station_id").alias("station_id"), pl.col("start_station_name").alias("station_name"))
        ends = df.filter(pl.col("end_station_id").is_not_null()).select(
            pl.col("end_station_id").alias("station_id"), pl.col("end_station_name").alias("station_name"))
        allst = pl.concat([starts, ends])
        unmatched = (allst.filter(~pl.col("station_id").is_in(known.implode()))
                     .group_by("station_id").agg(pl.col("station_name").first(), pl.len().cast(pl.UInt32).alias("trips"))
                     .sort("trips", descending=True))
        df = df.with_columns(
            (~pl.col("start_station_id").is_in(known.implode())
             | (pl.col("end_station_id").is_not_null() & ~pl.col("end_station_id").is_in(known.implode()))).alias("flag_unmatched_station")
        )
        dq.add("R10 station id not in GBFS short_name", "FLAG", df["flag_unmatched_station"].sum(),
               df.height, df.height, f"{unmatched.height} distinct ids (retired/temporary stations)")

    # R12 calendar keys -----------------------------------------------------------
    df = df.with_columns(
        pl.col("started_at").dt.date().alias("date"),
        pl.col("started_at").dt.hour().cast(pl.Int8).alias("hour"),
        pl.col("started_at").dt.weekday().cast(pl.Int8).alias("weekday"),   # 1=Mon .. 7=Sun
        (pl.col("started_at").dt.year() * 100 + pl.col("started_at").dt.month()).cast(pl.Int32).alias("month_key"),
        pl.col("ended_at").dt.date().alias("end_date"),
        pl.col("ended_at").dt.hour().cast(pl.Int8).alias("end_hour"),
    )
    return df, dq, unmatched


# ---------------------------------------------------------------- live status
def clean_status(status: pl.DataFrame, cfg: dict) -> tuple[pl.DataFrame, DQLog]:
    """Drop stale (> stale_after_minutes before snapshot) and not-installed stations."""
    dq = DQLog("station_status")
    n0 = status.height
    max_age = cfg["gbfs"]["stale_after_minutes"]
    s = status.with_columns(
        ((pl.col("snapshot_utc") - pl.col("last_reported_utc")).dt.total_seconds() / 60).alias("report_age_min")
    )
    n = s.height
    s = s.filter(pl.col("is_installed") == 1)
    dq.add("S01 station not installed", "REMOVE", n - s.height, n, s.height)
    n = s.height
    s = s.filter(pl.col("report_age_min") <= max_age)
    dq.add(f"S02 stale status (> {max_age} min old)", "REMOVE", n - s.height, n, s.height)
    dq.entries[0]["rows_in"] = n0
    return s, dq


def clean_station_info(info: pl.DataFrame, regions: pl.DataFrame, cfg: dict) -> tuple[pl.DataFrame, DQLog]:
    dq = DQLog("station_info")
    n0 = info.height
    s = info.with_columns(normalize_name("name").alias("name")).join(regions, on="region_id", how="left")
    excl_regions = cfg["gbfs"]["excluded_region_names"]
    n = s.height
    s = s.filter(~pl.col("region_name").is_in(excl_regions).fill_null(False))
    dq.add("I01 non-public region (8D / IC HQ / testzone)", "REMOVE", n - s.height, n, s.height)
    n = s.height
    s = s.filter(~pl.col("region_name").is_in(cfg["gbfs"].get("out_of_scope_region_names", [])).fill_null(False))
    dq.add("I01b out-of-scope region (Jersey City / Hoboken)", "REMOVE", n - s.height, n, s.height)
    n = s.height
    s = s.filter(~excluded_station_expr("name", cfg["cleaning"]["excluded_station_keywords"]))
    dq.add("I02 test/depot/lab station name", "REMOVE", n - s.height, n, s.height)
    n = s.height
    s = s.filter(pl.col("short_name") != "")
    dq.add("I03 missing short_name (cannot join trips)", "REMOVE", n - s.height, n, s.height)
    n = s.height
    s = s.unique(subset=["short_name"], keep="first", maintain_order=True)
    dq.add("I04 duplicate short_name", "REMOVE", n - s.height, n, s.height)
    dq.entries[0]["rows_in"] = n0
    return s, dq
