"""Extract live GBFS feeds (station_information, station_status, system_regions).

Always starts from the discovery file because feed URLs can change.
Note: Citi Bike's discovery file currently redirects to Lyft's GBFS v1.1 feeds
(gbfs.lyft.com/gbfs/1.1/bkn/...), not v2.3 - field names used here exist in both.
"""
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from common import get_json, get_logger, load_config

log = get_logger("extract_gbfs")


def discover_feeds(cfg: dict) -> dict[str, str]:
    disc = get_json(cfg["gbfs"]["discovery_url"], cfg["gbfs"]["timeout_s"])
    lang = cfg["gbfs"]["language"]
    feeds = {f["name"]: f["url"] for f in disc["data"][lang]["feeds"]}
    log.info("Discovered %d feeds (version %s)", len(feeds), disc.get("version", "?"))
    return feeds


def extract(cfg: dict | None = None) -> dict[str, pl.DataFrame]:
    """Return raw GBFS tables as polars DataFrames (IDs kept as strings)."""
    cfg = cfg or load_config()
    feeds = discover_feeds(cfg)
    t = cfg["gbfs"]["timeout_s"]

    info = get_json(feeds["station_information"], t)
    status = get_json(feeds["station_status"], t)
    regions = get_json(feeds["system_regions"], t)
    snapshot_utc = datetime.fromtimestamp(status["last_updated"], tz=timezone.utc)

    station_info = pl.DataFrame(
        [
            {
                "station_id": str(s["station_id"]),
                "short_name": str(s.get("short_name") or ""),
                "name": s.get("name"),
                "lat": float(s["lat"]),
                "lon": float(s["lon"]),
                "capacity": int(s.get("capacity") or 0),
                "region_id": str(s.get("region_id") or ""),
                "station_type": s.get("station_type"),
            }
            for s in info["data"]["stations"]
        ]
    )

    station_status = pl.DataFrame(
        [
            {
                "station_id": str(s["station_id"]),
                "num_bikes_available": int(s.get("num_bikes_available", 0)),
                "num_ebikes_available": int(s.get("num_ebikes_available", 0)),
                "num_bikes_disabled": int(s.get("num_bikes_disabled", 0)),
                "num_docks_available": int(s.get("num_docks_available", 0)),
                "num_docks_disabled": int(s.get("num_docks_disabled", 0)),
                "is_installed": int(s.get("is_installed", 0)),
                "is_renting": int(s.get("is_renting", 0)),
                "is_returning": int(s.get("is_returning", 0)),
                "last_reported": int(s.get("last_reported", 0)),
            }
            for s in status["data"]["stations"]
        ]
    ).with_columns(
        pl.from_epoch("last_reported", time_unit="s").dt.replace_time_zone("UTC").alias("last_reported_utc"),
        pl.lit(snapshot_utc).alias("snapshot_utc"),
    )

    system_regions = pl.DataFrame(
        [{"region_id": str(r["region_id"]), "region_name": r["name"]} for r in regions["data"]["regions"]]
    )

    log.info(
        "GBFS snapshot %s: %d stations info, %d status rows, %d regions",
        snapshot_utc.isoformat(), station_info.height, station_status.height, system_regions.height,
    )
    return {"station_info": station_info, "station_status": station_status, "system_regions": system_regions}


if __name__ == "__main__":
    for k, v in extract().items():
        print(k, v.shape)
        print(v.head(3))
