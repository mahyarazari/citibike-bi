"""Geography enrichment: NTA neighborhoods, population, station point-in-polygon join,
and a simplified map file for Power BI.

Sources (documented in README)
- 2020 Neighborhood Tabulation Areas (NYC Open Data 9nt8-h7nd, GeoJSON, EPSG:4326)
- Population: NYC Dept. of City Planning, "Decennial Census 2020 - core geographies"
  workbook (sheet "2020", GeoType = NTA2020, column Pop1 = total population).
  Why not ACS 5-year via the Census API? The API now requires a key; the 2020 decennial
  count is the most recent *official full count* published directly at NTA level, so no
  tract-to-NTA re-aggregation error is introduced.
- NTA types: 0 = residential; 5 = rikers; 6 = cemetery; 7 = airport; 8/9 = parks & other
  non-residential. Population may be 0 -> per-capita rates are NULL (never divide by 0).
"""
from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import polars as pl

from common import download, get_logger

log = get_logger("geo")

NTA_TYPE = {0: "Residential", 5: "Rikers Island", 6: "Cemetery", 7: "Airport", 8: "Park / open space",
            9: "Park / open space"}
METRIC_CRS = "EPSG:2263"  # NY State Plane Long Island (US feet)
FT2_PER_KM2 = 10_763_910.4


def _ensure_sources(cfg: dict) -> tuple[Path, Path]:
    geo_dir = cfg["paths"]["raw_dir"] / "geo"
    geo_dir.mkdir(parents=True, exist_ok=True)
    nta, pop = geo_dir / "nta2020.geojson", geo_dir / "dcp_census2020_core_geographies.xlsx"
    if not nta.exists():
        log.info("Downloading NTA polygons")
        download(cfg["geo"]["nta_geojson_url"], nta)
    if not pop.exists():
        log.info("Downloading DCP 2020 census workbook")
        download(cfg["geo"]["population_xlsx_url"], pop)
    return nta, pop


def load_neighborhoods(cfg: dict) -> gpd.GeoDataFrame:
    nta_path, pop_path = _ensure_sources(cfg)
    g = gpd.read_file(nta_path)[["nta2020", "ntaname", "boroname", "ntatype", "geometry"]]
    g = g.rename(columns={"nta2020": "nta_code", "ntaname": "nta_name", "boroname": "borough"})
    g["ntatype"] = g["ntatype"].astype(int)
    g["nta_type"] = g["ntatype"].map(NTA_TYPE).fillna("Other")

    pop = pd.read_excel(pop_path, sheet_name="2020", usecols=["GeoType", "GeoID", "Pop1"])
    pop = pop[pop["GeoType"] == "NTA2020"].rename(columns={"GeoID": "nta_code", "Pop1": "population"})
    g = g.merge(pop[["nta_code", "population"]], on="nta_code", how="left")
    missing = g["population"].isna().sum()
    if missing:
        log.warning("%d NTAs have no population row -> treated as 0", missing)
    g["population"] = g["population"].fillna(0).astype(int)
    g["area_km2"] = g.to_crs(METRIC_CRS).area / FT2_PER_KM2
    return g


def join_stations(stations: pl.DataFrame, nbhd: gpd.GeoDataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Point-in-polygon join. Returns (short_name -> nta mapping, stations outside all polygons)."""
    s = stations.filter(pl.col("lat").is_not_null()).select("short_name", "name", "lat", "lon").to_pandas()
    pts = gpd.GeoDataFrame(s, geometry=gpd.points_from_xy(s["lon"], s["lat"]), crs="EPSG:4326")
    j = gpd.sjoin(pts, nbhd[["nta_code", "nta_name", "borough", "geometry"]], how="left", predicate="within")
    j = j.drop_duplicates("short_name")
    outside = pl.from_pandas(j[j["nta_code"].isna()][["short_name", "name", "lat", "lon"]])
    mapping = pl.from_pandas(j[["short_name", "nta_code", "nta_name", "borough"]])
    log.info("Spatial join: %d stations, %d outside all NTA polygons (e.g. Jersey City / piers)",
             len(j), outside.height)
    return mapping, outside


def build_dim_neighborhood(nbhd: gpd.GeoDataFrame, dim_station: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    active = dim_station.filter(pl.col("is_active"))
    counts = active.group_by("nta_code").agg(pl.len().alias("station_count"),
                                             pl.col("capacity").sum().alias("dock_count"))
    # service area: NTAs containing or within buffer of an active station
    buf_ft = cfg["geo"]["service_area_buffer_km"] * 3280.84
    st = active.select("lat", "lon").to_pandas()
    pts = gpd.GeoSeries(gpd.points_from_xy(st["lon"], st["lat"]), crs="EPSG:4326").to_crs(METRIC_CRS)
    area = pts.buffer(buf_ft).union_all()
    in_service = nbhd.to_crs(METRIC_CRS).intersects(area)

    d = pl.from_pandas(pd.DataFrame({
        "nta_code": nbhd["nta_code"], "nta_name": nbhd["nta_name"], "borough": nbhd["borough"],
        "nta_type": nbhd["nta_type"], "population": nbhd["population"],
        "area_km2": nbhd["area_km2"].round(3), "in_service_area": in_service.values,
    }))
    d = d.join(counts, on="nta_code", how="left").with_columns(
        pl.col("station_count").fill_null(0).cast(pl.Int32),
        pl.col("dock_count").fill_null(0).cast(pl.Int32),
    ).with_columns(
        pl.when(pl.col("population") > 0).then(pl.col("dock_count") / pl.col("population") * 1000)
        .otherwise(None).alias("docks_per_1k_residents"),
        pl.when(pl.col("area_km2") > 0).then(pl.col("station_count") / pl.col("area_km2"))
        .otherwise(None).alias("stations_per_km2"),
        (pl.col("population") > 0).alias("has_population"),
    )
    # explicit row for stations outside all polygons (no population)
    extra = pl.DataFrame({"nta_code": ["OUTSIDE"], "nta_name": ["Outside NYC NTAs"], "borough": ["Outside NYC"],
                          "nta_type": ["Outside"], "population": [0], "area_km2": [None], "in_service_area": [True]})
    extra = extra.join(counts, on="nta_code", how="left").with_columns(
        pl.col("station_count").fill_null(0).cast(pl.Int32), pl.col("dock_count").fill_null(0).cast(pl.Int32),
        pl.lit(None, pl.Float64).alias("docks_per_1k_residents"), pl.lit(None, pl.Float64).alias("stations_per_km2"),
        pl.lit(False).alias("has_population"))
    return pl.concat([d, extra], how="vertical_relaxed").sort("nta_code")


def export_map(nbhd: gpd.GeoDataFrame, dim_nbhd: pl.DataFrame, cfg: dict) -> dict:
    """Write simplified TopoJSON (Shape Map / Deneb) + GeoJSON for Power BI."""
    import topojson as tp

    svc = dim_nbhd.select("nta_code", "in_service_area").to_pandas()
    g = nbhd[["nta_code", "nta_name", "borough", "geometry"]].merge(svc, on="nta_code", how="left")
    g["in_service_area"] = g["in_service_area"].fillna(False).astype(bool)
    topo = tp.Topology(g, prequantize=1e5, toposimplify=cfg["geo"]["simplify_tolerance_deg"], object_name="nta")
    out_dir = cfg["paths"]["maps_dir"]
    topo_path, geo_path = out_dir / "nyc_nta.json", out_dir / "nyc_nta.geojson"
    topo_path.write_text(topo.to_json(), encoding="utf-8")
    gj = json.loads(topo.to_geojson())
    # round coordinates to 5 decimals (~1 m) to keep the GeoJSON small
    def rnd(c):
        return [rnd(x) for x in c] if isinstance(c[0], list) else [round(c[0], 5), round(c[1], 5)]
    for f in gj["features"]:
        f["geometry"]["coordinates"] = rnd(f["geometry"]["coordinates"])
        f.pop("id", None)
    geo_path.write_text(json.dumps(gj, separators=(",", ":")), encoding="utf-8")
    sizes = {"topojson_kb": round(topo_path.stat().st_size / 1024), "geojson_kb": round(geo_path.stat().st_size / 1024)}
    log.info("Map files written: %s", sizes)
    return sizes
