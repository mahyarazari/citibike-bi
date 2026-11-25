"""Unit tests for the trip / station cleaning rules (pipeline/clean.py)."""
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from clean import clean_station_info, clean_status, clean_trips, haversine_km, normalize_name
from common import load_config


@pytest.fixture(scope="module")
def cfg():
    return load_config()


def trip(ride_id="r1", start="2026-10-01 08:00:00", end="2026-10-01 08:15:00",
         s_id="6560.01", e_id="5905.06", s_name="Broadway & W 41 St", e_name="W 21 St & 6 Ave",
         s_lat="40.755", s_lng="-73.986", e_lat="40.741", e_lng="-73.994", rt="classic_bike", mc="member"):
    return {"ride_id": ride_id, "rideable_type": rt, "started_at": start, "ended_at": end,
            "start_station_name": s_name, "start_station_id": s_id, "end_station_name": e_name,
            "end_station_id": e_id, "start_lat": s_lat, "start_lng": s_lng, "end_lat": e_lat,
            "end_lng": e_lng, "member_casual": mc}


def run(rows, cfg, known=None):
    df = pl.DataFrame(rows, schema={k: pl.String for k in rows[0]})
    out, dq, unmatched = clean_trips(df, cfg, known)
    return out, {e["rule"][:3]: e for e in dq.entries}, unmatched


def test_duplicates_removed(cfg):
    out, dq, _ = run([trip("a"), trip("a"), trip("b")], cfg)
    assert out.height == 2
    assert dq["R01"]["rows_affected"] == 1


def test_station_ids_stay_strings(cfg):
    out, _, _ = run([trip(s_id="5905.10")], cfg)
    assert out.schema["start_station_id"] == pl.String
    assert out["start_station_id"][0] == "5905.10"  # trailing zero preserved (never cast to float)


def test_timezone_and_calendar_keys(cfg):
    out, _, _ = run([trip(start="2026-10-04 23:30:00", end="2026-10-04 23:45:00")], cfg)
    assert str(out["started_at"].dtype.time_zone) == "America/New_York"
    assert out["hour"][0] == 23
    assert out["weekday"][0] == 7  # Sunday
    assert out["month_key"][0] == 202610


def test_unparseable_timestamp_removed(cfg):
    out, dq, _ = run([trip("a", start="not a date"), trip("b")], cfg)
    assert out.height == 1 and dq["R02"]["rows_affected"] == 1


def test_duration_bounds(cfg):
    rows = [
        trip("short", end="2026-10-01 08:00:30"),                    # 0.5 min
        trip("long", end="2026-10-02 09:00:00"),                     # 25 h
        trip("ok"),
        trip("neg", start="2026-10-01 09:00:00", end="2026-10-01 08:00:00"),
    ]
    out, dq, _ = run(rows, cfg)
    assert out["ride_id"].to_list() == ["ok"]
    assert dq["R03"]["rows_affected"] == 2  # short + negative
    assert dq["R04"]["rows_affected"] == 1
    assert out["duration_min"][0] == pytest.approx(15)


def test_missing_end_station_flagged_not_dropped(cfg):
    out, dq, _ = run([trip("a", e_id="", e_name="", e_lat="", e_lng=""), trip("b")], cfg)
    assert out.height == 2
    assert out.filter(pl.col("ride_id") == "a")["flag_unreturned"][0] is True
    assert dq["R06"]["rows_affected"] == 1


def test_out_of_bbox_coordinates_nulled_and_flagged(cfg):
    out, dq, _ = run([trip("a", e_lat="0", e_lng="0")], cfg)
    assert out["end_lat"][0] is None and out["flag_bad_coords"][0] is True
    assert out["distance_km"][0] is None
    assert dq["R08"]["rows_affected"] == 1


@pytest.mark.parametrize("name", ["NYCBS Depot - DYR", "Lab - NYC", "MTL-ECO5-LAB", "Mobile Station 1", "warehouse X"])
def test_test_depot_stations_excluded(cfg, name):
    out, dq, _ = run([trip("a", s_name=name), trip("b")], cfg)
    assert out["ride_id"].to_list() == ["b"]
    assert dq["R07"]["rows_affected"] == 1


def test_station_name_normalized(cfg):
    out, _, _ = run([trip(s_name="  Broadway   &W 41 St ")], cfg)
    assert out["start_station_name"][0] == "Broadway & W 41 St"


def test_haversine_known_distance():
    df = pl.DataFrame({"a": [40.7484], "b": [-73.9857], "c": [40.7580], "d": [-73.9855]})  # ESB -> Times Sq
    km = df.select(haversine_km(pl.col("a"), pl.col("b"), pl.col("c"), pl.col("d")))[0, 0]
    assert km == pytest.approx(1.068, abs=0.01)


def test_implausible_speed_flag_only_for_classic(cfg):
    # ~8 km in 2 minutes = 240 km/h
    fast = dict(e_lat="40.80", e_lng="-73.95", end="2026-10-01 08:02:00")
    out, dq, _ = run([trip("c", **fast), trip("e", rt="electric_bike", **fast)], cfg)
    flags = dict(zip(out["ride_id"], out["flag_implausible_speed"]))
    assert flags == {"c": True, "e": False}
    assert dq["R09"]["rows_affected"] == 1


def test_unmatched_station_reported(cfg):
    out, dq, unmatched = run([trip(s_id="9999.99"), trip("b")], cfg, known={"6560.01", "5905.06"})
    assert unmatched["station_id"].to_list() == ["9999.99"]
    assert dq["R10"]["rows_affected"] == 1


def test_dq_rows_in_out_are_consistent(cfg):
    _, dq, _ = run([trip("a"), trip("a"), trip("b", end="2026-10-01 08:00:10")], cfg)
    for e in dq.values():
        if e["action"] == "REMOVE":
            assert e["rows_in"] - e["rows_affected"] == e["rows_out"]


# ------------------------------------------------------------------ live status
def test_status_drops_stale_and_not_installed(cfg):
    snap = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
    st = pl.DataFrame({
        "station_id": ["a", "b", "c"],
        "is_installed": [1, 0, 1],
        "last_reported_utc": [snap - timedelta(minutes=5), snap, snap - timedelta(hours=3)],
    }).with_columns(pl.lit(snap).alias("snapshot_utc"))
    out, dq = clean_status(st, cfg)
    assert out["station_id"].to_list() == ["a"]
    assert [e["rows_affected"] for e in dq.entries] == [1, 1]


def test_station_info_filters(cfg):
    info = pl.DataFrame({
        "station_id": ["1", "2", "3", "4", "5"],
        "short_name": ["100.01", "200.02", "", "100.01", "300.03"],
        "name": ["A St", "B St", "C St", "A St dup", "NYCBS Depot"],
        "region_id": ["71", "190", "71", "71", "71"],
    })
    regions = pl.DataFrame({"region_id": ["71", "190"], "region_name": ["NYC District", "testzone"]})
    out, _ = clean_station_info(info, regions, cfg)
    assert out["station_id"].to_list() == ["1"]


def test_normalize_name_expression():
    df = pl.DataFrame({"n": ["W 21 St&6 Ave", "  a  b "]})
    assert df.select(normalize_name("n"))["n"].to_list() == ["W 21 St & 6 Ave", "a b"]


def test_trailing_underscore_id_repaired(cfg):
    out, dq, _ = run([trip(s_id="6517.08_")], cfg)
    assert out["start_station_id"][0] == "6517.08"
    assert dq["R05"]["rows_affected"] == 0 and any(e for k, e in dq.items())


@pytest.mark.parametrize("sid", ["SYS038", "1234.56", "JC115", "HB101"])
def test_internal_and_nj_station_ids_excluded(cfg, sid):
    out, _, _ = run([trip("a", s_id=sid, s_name="Some Place"), trip("b")], cfg)
    assert out["ride_id"].to_list() == ["b"]


def test_morgan_ave_is_not_excluded(cfg):
    out, _, _ = run([trip(s_name="Morgan Ave & Maspeth Ave")], cfg)
    assert out.height == 1
