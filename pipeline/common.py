"""Shared helpers: config loading, logging, HTTP with retries, output writers."""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

_session = requests.Session()
_session.headers["User-Agent"] = "citibike-bi-pipeline/1.0 (educational project)"


def load_config(path: Path = CONFIG_PATH) -> dict:
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Resolve every path relative to the repository root
    cfg["paths"] = {k: ROOT / v for k, v in cfg["paths"].items()}
    for p in ("raw_dir", "work_dir", "clean_dir", "agg_dir", "sample_dir", "maps_dir"):
        cfg["paths"][p].mkdir(parents=True, exist_ok=True)
    return cfg


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=os.environ.get("LOG_LEVEL", "INFO"),
            format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
            datefmt="%H:%M:%S",
        )
    return logger


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout, requests.HTTPError)),
    reraise=True,
)
def http_get(url: str, timeout: int = 60, stream: bool = False) -> requests.Response:
    """GET with exponential backoff (2s .. 60s, 5 attempts)."""
    r = _session.get(url, timeout=timeout, stream=stream)
    if r.status_code >= 500 or r.status_code == 429:
        r.raise_for_status()
    if r.status_code >= 400:
        # 4xx (other than 429) is not retryable
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r


def get_json(url: str, timeout: int = 30) -> dict:
    return http_get(url, timeout=timeout).json()


def download(url: str, dest: Path, chunk: int = 1 << 20, max_attempts: int = 30) -> Path:
    """Resumable streaming download (HTTP Range), atomic rename when complete.

    A dropped/slow connection resumes from the bytes already in <dest>.part instead
    of starting over; a .part file left by an interrupted run is resumed too.
    """
    import time

    tmp = dest.with_suffix(dest.suffix + ".part")
    total = None
    for attempt in range(1, max_attempts + 1):
        have = tmp.stat().st_size if tmp.exists() else 0
        if total is not None and have >= total:
            break
        headers = {"Range": f"bytes={have}-"} if have else {}
        try:
            with _session.get(url, headers=headers, timeout=(30, 120), stream=True) as r:
                if r.status_code == 416:  # already complete
                    break
                r.raise_for_status()
                if have and r.status_code != 206:  # server ignored Range -> restart
                    have = 0
                length = int(r.headers.get("Content-Length", 0))
                total = have + length if length else total
                with open(tmp, "ab" if have else "wb") as f:
                    for block in r.iter_content(chunk):
                        f.write(block)
            if total is None or tmp.stat().st_size >= total:
                break
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as e:
            got = tmp.stat().st_size if tmp.exists() else 0
            logging.getLogger("download").warning(
                "%s: attempt %d interrupted at %.0f MB (%s) - resuming", dest.name, attempt, got / 1e6, type(e).__name__)
            time.sleep(min(60, 2 * attempt))
    else:
        raise RuntimeError(f"Download of {url} failed after {max_attempts} attempts")
    tmp.replace(dest)
    return dest


def write_table(df, name: str, cfg: dict, csv: bool = True) -> dict:
    """Write a polars/pandas DataFrame to data/clean as Parquet (+ CSV fallback).

    Returns size info; raises if a file exceeds the configured GitHub limit.
    """
    import polars as pl

    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df)
    out = cfg["paths"]["clean_dir"]
    pq = out / f"{name}.parquet"
    df.write_parquet(pq, compression="zstd")
    limit = cfg["limits"]["max_file_mb"]
    pq_mb = pq.stat().st_size / 1e6
    if pq_mb > limit:
        raise ValueError(f"{name}.parquet = {pq_mb:.1f} MB exceeds {limit} MB limit")
    info = {"table": name, "rows": df.height, "parquet_mb": round(pq_mb, 2), "csv_mb": None}
    c = out / f"{name}.csv"
    if csv:
        df.write_csv(c)
        csv_mb = c.stat().st_size / 1e6
        if csv_mb > limit:
            # Parquet is the primary format; CSV fallback only when it fits GitHub limits
            c.unlink()
        else:
            info["csv_mb"] = round(csv_mb, 2)
    elif c.exists():
        c.unlink()
    return info


def read_json(path: Path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
