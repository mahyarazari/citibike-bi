"""Hourly weather for NYC (Central Park) from Open-Meteo (free, no key).

Uses the historical archive API for the trip window. Times are requested in
America/New_York so they join directly to dim_date x dim_hour.
"""
from __future__ import annotations

from datetime import date

import polars as pl

from common import get_json, get_logger

log = get_logger("weather")
URL = ("https://archive-api.open-meteo.com/v1/archive?latitude=40.7812&longitude=-73.9665"
       "&start_date={s}&end_date={e}&hourly=temperature_2m,precipitation,wind_speed_10m,weather_code"
       "&timezone=America%2FNew_York")


def fetch(start: date, end: date) -> pl.DataFrame:
    d = get_json(URL.format(s=start.isoformat(), e=end.isoformat()), timeout=60)["hourly"]
    df = pl.DataFrame(d).with_columns(pl.col("time").str.to_datetime("%Y-%m-%dT%H:%M"))
    df = df.select(
        pl.col("time").dt.date().alias("date"),
        pl.col("time").dt.hour().cast(pl.Int8).alias("hour"),
        pl.col("temperature_2m").alias("temp_c"),
        pl.col("precipitation").alias("precip_mm"),
        pl.col("wind_speed_10m").alias("wind_kmh"),
        "weather_code",
    ).with_columns(
        pl.when(pl.col("precip_mm") >= 0.5).then(pl.lit("Rain"))
        .when(pl.col("temp_c") < 5).then(pl.lit("Cold"))
        .when(pl.col("temp_c") > 30).then(pl.lit("Hot"))
        .otherwise(pl.lit("Fair")).alias("weather_class")
    ).drop_nulls("temp_c")
    log.info("Weather: %d hourly rows %s..%s", df.height, start, end)
    return df
