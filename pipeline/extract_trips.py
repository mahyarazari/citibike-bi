"""Discover, download and incrementally load monthly Citi Bike trip files.

- Lists the public S3 bucket via its XML API (paginated, list-type=2).
- Keeps NYC monthly files named YYYYMM-citibike-tripdata*.zip (JC- files skipped by default).
- Picks the last N *complete* months (months before the current month).
- A manifest (data/manifest.json) records processed months + ETag, so re-runs only
  download new or changed months (idempotent, incremental).
- Each month's ZIP may hold several CSVs (and sometimes nested ZIPs); all are read
  with every column as VARCHAR so station IDs like "5905.06" are never cast to float.
- Output: one Parquet per month in data/work/trips_raw_YYYYMM.parquet (git-ignored).
"""
from __future__ import annotations

import re
import shutil
import xml.etree.ElementTree as ET
import zipfile
from datetime import date
from pathlib import Path
from urllib.parse import quote

import duckdb

from common import download, get_logger, http_get, load_config, read_json, write_json

log = get_logger("extract_trips")

NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
MONTH_RE = re.compile(r"^(JC-)?(\d{6})-citibike-tripdata.*\.zip$")
TRIP_COLUMNS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
]


def list_bucket(bucket_url: str) -> list[dict]:
    """Return every object in the bucket: key, size, etag, last_modified."""
    objs, token = [], None
    while True:
        url = f"{bucket_url}?list-type=2"
        if token:
            url += "&continuation-token=" + quote(token, safe="")
        root = ET.fromstring(http_get(url, timeout=60).content)
        for c in root.findall("s3:Contents", NS):
            objs.append({
                "key": c.findtext("s3:Key", namespaces=NS),
                "size": int(c.findtext("s3:Size", "0", NS)),
                "etag": (c.findtext("s3:ETag", "", NS) or "").strip('"'),
                "last_modified": c.findtext("s3:LastModified", namespaces=NS),
            })
        token = root.findtext("s3:NextContinuationToken", namespaces=NS)
        if not token:
            break
    return objs


def monthly_files(objs: list[dict], include_jc: bool) -> dict[str, dict]:
    """Map 'YYYYMM' -> object for NYC monthly ZIPs."""
    out = {}
    for o in objs:
        m = MONTH_RE.match(o["key"])
        if not m:
            continue
        if m.group(1) and not include_jc:
            continue
        out[m.group(2)] = o
    return out


def target_months(available: dict[str, dict], n: int, today: date | None = None) -> list[str]:
    """Last n complete months that exist in the bucket (current month excluded)."""
    today = today or date.today()
    current = f"{today.year}{today.month:02d}"
    complete = sorted(k for k in available if k < current)
    return complete[-n:]


def _extract_csvs(zip_path: Path, dest: Path) -> list[Path]:
    """Extract all CSVs from a ZIP, recursing into nested ZIPs; ignore __MACOSX."""
    found: list[Path] = []
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            n = info.filename
            if info.is_dir() or "__MACOSX" in n:
                continue
            if n.lower().endswith(".csv"):
                target = dest / Path(n).name
                with z.open(info) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst, 1 << 20)
                found.append(target)
            elif n.lower().endswith(".zip"):
                inner = dest / Path(n).name
                with z.open(info) as src, open(inner, "wb") as dst:
                    shutil.copyfileobj(src, dst, 1 << 20)
                found += _extract_csvs(inner, dest)
                inner.unlink()
    return found


def load_month(month: str, obj: dict, cfg: dict) -> dict:
    """Download one month, convert its CSV(s) to a single Parquet, return stats."""
    raw, work = cfg["paths"]["raw_dir"], cfg["paths"]["work_dir"]
    zip_path = raw / obj["key"]
    tmp_dir = raw / f"tmp_{month}"
    tmp_dir.mkdir(exist_ok=True)
    try:
        log.info("Downloading %s (%.0f MB)", obj["key"], obj["size"] / 1e6)
        download(f"{cfg['trips']['bucket_url']}/{obj['key']}", zip_path)
        csvs = _extract_csvs(zip_path, tmp_dir)
        if cfg["trips"]["delete_zip_after_load"]:
            zip_path.unlink()
        log.info("%s: %d CSV file(s) inside", month, len(csvs))

        out = work / f"trips_raw_{month}.parquet"
        con = duckdb.connect()
        files = [str(p).replace("\\", "/") for p in csvs]
        cols = ", ".join(f'"{c}"' for c in TRIP_COLUMNS)
        # all_varchar: IDs stay text; union_by_name: tolerate column-order differences
        con.execute(
            f"""
            COPY (
              SELECT {cols}, regexp_extract(filename, '[^/\\\\]+$') AS source_file
              FROM read_csv({files}, all_varchar=true, header=true, union_by_name=true,
                            filename=true, quote='"', ignore_errors=false)
            ) TO '{str(out).replace(chr(92), '/')}' (FORMAT parquet, COMPRESSION zstd)
            """
        )
        rows = con.execute(f"SELECT count(*) FROM '{str(out).replace(chr(92), '/')}'").fetchone()[0]
        con.close()
        log.info("%s: %s rows -> %s", month, f"{rows:,}", out.name)
        return {"month": month, "key": obj["key"], "etag": obj["etag"], "rows_raw": rows,
                "csv_files": len(csvs), "parquet": out.name}
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def run(cfg: dict | None = None, force: bool = False, months: int | None = None) -> dict:
    """Incremental load. Returns the manifest."""
    cfg = cfg or load_config()
    tcfg = cfg["trips"]
    manifest_path = cfg["paths"]["manifest"]
    manifest = read_json(manifest_path, {"months": {}})

    available = monthly_files(list_bucket(tcfg["bucket_url"]), tcfg["include_jersey_city"])
    wanted = target_months(available, months or tcfg["months_to_keep"])
    log.info("Target months: %s", ", ".join(wanted))

    for m in wanted:
        obj = available[m]
        done = manifest["months"].get(m)
        # A month is "done" when its cleaned per-month aggregates exist (committed to git),
        # so incremental loading also works on a fresh CI checkout without raw files.
        agg_ok = (cfg["paths"]["agg_dir"] / m / "_SUCCESS").exists()
        if done and done.get("etag") == obj["etag"] and agg_ok and not force:
            log.info("%s already processed (etag unchanged) - skip", m)
            continue
        entry = load_month(m, obj, cfg)
        entry["status"] = "extracted"  # becomes "processed" after clean + aggregate
        manifest["months"][m] = entry
        write_json(manifest_path, manifest)  # save after every month (resumable)

    # Drop months that fell out of the rolling window
    for m in list(manifest["months"]):
        if m not in wanted:
            log.info("%s outside the %d-month window - removing", m, len(wanted))
            (cfg["paths"]["work_dir"] / f"trips_raw_{m}.parquet").unlink(missing_ok=True)
            shutil.rmtree(cfg["paths"]["agg_dir"] / m, ignore_errors=True)
            del manifest["months"][m]
    manifest["window"] = wanted
    write_json(manifest_path, manifest)
    return manifest


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--months", type=int)
    a = ap.parse_args()
    print(run(force=a.force, months=a.months))
