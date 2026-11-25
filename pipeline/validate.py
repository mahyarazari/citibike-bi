"""Pandera schemas for raw and clean data. A failed check stops the run (fail fast)."""
from __future__ import annotations

import pandera.polars as pa
import polars as pl

STATION_ID = r"^\d+\.\d+$|^[A-Za-z0-9 _\-.]+$"  # short_name like "5905.06" (kept as text)

RawTrips = pa.DataFrameSchema(
    {
        "ride_id": pa.Column(pl.String, nullable=False),
        "rideable_type": pa.Column(pl.String, nullable=True),
        "started_at": pa.Column(pl.String, nullable=False),
        "ended_at": pa.Column(pl.String, nullable=False),
        "start_station_id": pa.Column(pl.String, nullable=True),
        "end_station_id": pa.Column(pl.String, nullable=True),
        "member_casual": pa.Column(pl.String, pa.Check.isin(["member", "casual"]), nullable=True),
    },
    strict=False,
    name="raw_trips",
)

CleanTrips = pa.DataFrameSchema(
    {
        "ride_id": pa.Column(pl.String, unique=True),
        "start_station_id": pa.Column(pl.String, nullable=False),
        "end_station_id": pa.Column(pl.String, nullable=True),
        "duration_min": pa.Column(pl.Float64, pa.Check.in_range(1, 1440)),
        "start_lat": pa.Column(pl.Float64, pa.Check.in_range(40.4, 41.0), nullable=True),
        "start_lng": pa.Column(pl.Float64, pa.Check.in_range(-74.3, -73.6), nullable=True),
        "end_lat": pa.Column(pl.Float64, pa.Check.in_range(40.4, 41.0), nullable=True),
        "end_lng": pa.Column(pl.Float64, pa.Check.in_range(-74.3, -73.6), nullable=True),
        "hour": pa.Column(pl.Int8, pa.Check.in_range(0, 23)),
        "weekday": pa.Column(pl.Int8, pa.Check.in_range(1, 7)),
        "flag_unreturned": pa.Column(pl.Boolean),
    },
    strict=False,
    name="clean_trips",
)

StationInfo = pa.DataFrameSchema(
    {
        "station_id": pa.Column(pl.String, unique=True),
        "short_name": pa.Column(pl.String, unique=True, nullable=False),
        "lat": pa.Column(pl.Float64, pa.Check.in_range(40.4, 41.0)),
        "lon": pa.Column(pl.Float64, pa.Check.in_range(-74.3, -73.6)),
        "capacity": pa.Column(pl.Int64, pa.Check.ge(0)),
    },
    strict=False,
    name="station_info_clean",
)

StationStatus = pa.DataFrameSchema(
    {
        "station_id": pa.Column(pl.String, nullable=False),
        "num_bikes_available": pa.Column(pl.Int64, pa.Check.ge(0)),
        "num_docks_available": pa.Column(pl.Int64, pa.Check.ge(0)),
        "is_installed": pa.Column(pl.Int64, pa.Check.isin([0, 1])),
    },
    strict=False,
    name="station_status",
)

LiveStation = pa.DataFrameSchema(
    {
        "fill_ratio": pa.Column(pl.Float64, pa.Check.ge(0), nullable=True),
        "status_class": pa.Column(pl.String, pa.Check.isin(["Empty", "Low", "Balanced", "High", "Full"])),
        "rebalance_priority_score": pa.Column(pl.Float64, pa.Check.in_range(0, 100)),
    },
    strict=False,
    name="live_station",
)


def check(schema: pa.DataFrameSchema, df: pl.DataFrame, sample: int | None = None) -> pl.DataFrame:
    """Validate (optionally on a random sample for very large frames); raises SchemaError."""
    target = df.sample(n=min(sample, df.height), seed=42) if sample and df.height > sample else df
    schema.validate(target, lazy=True)
    return df
