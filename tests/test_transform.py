"""Unit tests for live-operations logic (status class, time-to-empty, priority)."""
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

import transform as T
from common import load_config


@pytest.fixture(scope="module")
def cfg():
    return load_config()


def test_status_class_thresholds():
    df = pl.DataFrame({"bikes": [0, 1, 10, 19, 15], "docks": [20, 19, 10, 1, 0], "cap": [20, 20, 20, 20, 15]})
    out = df.select(T.classify(pl.col("bikes") / pl.col("cap"), pl.col("bikes"), pl.col("docks"), 0.10, 0.90))
    assert out.to_series().to_list() == ["Empty", "Low", "Balanced", "High", "Full"]


def _history(bikes_series, step_min=30, docks_total=20):
    t0 = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i, b in enumerate(bikes_series):
        rows.append({"snapshot_utc": t0 + timedelta(minutes=step_min * i), "station_id": "s1",
                     "num_bikes_available": b, "num_ebikes_available": 0, "num_bikes_disabled": 0,
                     "num_docks_available": docks_total - b, "num_docks_disabled": 0, "is_renting": 1,
                     "is_returning": 1, "last_reported_utc": t0})
    return pl.DataFrame(rows)


DIM = pl.DataFrame({"station_id": ["s1"], "station_key": [1], "short_name": ["100.01"], "name": ["A"],
                    "capacity": [20], "lat": [40.75], "lon": [-73.98], "nta_code": ["MN01"],
                    "borough": ["Manhattan"], "region_name": ["NYC District"], "is_active": [True]})


def test_draining_station_time_to_empty(cfg):
    # 16 -> 12 -> 8 -> 4 bikes every 30 min = -8 bikes/hour -> 4 bikes left = 30 min to empty
    live = T.live_station_metrics(_history([16, 12, 8, 4]), DIM, None, cfg)
    r = live.row(0, named=True)
    assert r["net_flow_per_hour"] == pytest.approx(-8)
    assert r["est_minutes_to_empty"] == pytest.approx(30)
    assert r["est_minutes_to_full"] is None
    assert r["status_class"] == "Balanced"
    assert 0 < r["rebalance_priority_score"] <= 100
    assert r["bikes_to_target"] == 6  # to 50 % of 20


def test_empty_now_has_max_urgency(cfg):
    live = T.live_station_metrics(_history([0]), DIM, None, cfg)
    r = live.row(0, named=True)
    assert r["status_class"] == "Empty" and r["est_minutes_to_empty"] == 0 and r["urgency"] == 1.0


def test_history_rolling_window_and_dedup():
    h = _history([5, 6, 7])
    again = h.tail(1)
    out = T.update_status_history(h, again, retention_days=14)
    assert out.height == 3
    old = h.with_columns(pl.col("snapshot_utc") - timedelta(days=20))
    out = T.update_status_history(old, again, retention_days=14)
    assert out.height == 1


def test_dim_hour_day_parts():
    d = T.build_dim_hour()
    assert d.height == 24
    assert d.filter(pl.col("hour") == 8)["day_part"][0] == "AM Peak"
    assert d.filter(pl.col("hour") == 0)["hour_label"][0] == "12 AM"
