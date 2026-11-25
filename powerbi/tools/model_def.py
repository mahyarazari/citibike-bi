"""Semantic model definition: tables, relationships, measures (DAX).

Kept separate from the generator so the business logic (DAX) is easy to review.
Table columns are read from the Parquet files in data/clean, so the model always
matches what the pipeline publishes.
"""

# --------------------------------------------------------------------------- tables
# name -> source file in data/clean (without .parquet), hidden columns, display props
FACT_TABLES = {
    "fact_trips_hourly": "fact_trips_hourly",
    "fact_station_hourly": "fact_station_hourly",
    "fact_od_monthly": "fact_od_monthly",
    "fact_top_routes": "fact_top_routes",
    "fact_customer_profile": "fact_customer_profile",
    "fact_casual_repeat_od": "fact_casual_repeat_od",
    "fact_station_profile": "fact_station_profile",
    "fact_station_live": "fact_station_live",
    "fact_station_status": "fact_station_status",
    "fact_rebalance_list": "fact_rebalance_list",
    "fact_neighborhood_live": "fact_neighborhood_live",
    "fact_weather_hourly": "fact_weather_hourly",
    "fact_trip_cleaning": "fact_trip_cleaning",
    "fact_data_quality": "fact_data_quality",
    "fact_pipeline_runs": "fact_pipeline_runs",
    "fact_unmatched_stations": "fact_unmatched_stations",
    "fact_stations_outside_nta": "fact_stations_outside_nta",
}
DIM_TABLES = {
    "dim_date": "dim_date",
    "dim_hour": "dim_hour",
    "dim_station": "dim_station",
    "dim_neighborhood": "dim_neighborhood",
    "dim_region": "dim_region",
    "meta_refresh": "meta_refresh",
}
HIDDEN_COLUMN_SUFFIXES = ("_key", "_id", "_utc")
HIDDEN_COLUMNS = {"station_id", "short_name_hidden", "date_key", "weather_code", "source_file"}
VISIBLE_OVERRIDES = {("dim_station", "station_key"): False}  # (table, col): hidden?
SORT_BY = {
    ("dim_date", "month_name"): "month",
    ("dim_date", "weekday_name"): "weekday_num",
    ("dim_hour", "hour_label"): "hour",
    ("dim_hour", "day_part"): "day_part_order",
}
PERCENT_COLUMNS = {"fill_ratio", "pct_balanced", "pct_balanced_1h_ago", "pct_balanced_change_1h", "member_share",
                   "demand_index", "urgency", "trips_vs_normal"}

# Static helper tables (M #table literals)
HELPER_TABLES = {
    "Map Metric": {
        "columns": [("metric_id", "int64"), ("Metric", "string")],
        "m": '#table(type table [metric_id = Int64.Type, Metric = text], {'
             '{1, "Live balance"}, {2, "Bikes per 1k residents"}, {3, "Trips per 1k residents"}, '
             '{4, "Week-over-week growth"}, {5, "Utilization per dock"}})',
        "sort": {"Metric": "metric_id"},
        "hidden": {"metric_id"},
    },
    "Membership Price": {
        "columns": [("Price", "int64")],
        "m": '#table(type table [Price = Int64.Type], List.Transform(List.Numbers(100, 61, 5), each {_}))',
    },
    "Conversion Rate": {
        "columns": [("Rate", "double")],
        "m": '#table(type table [Rate = number], List.Transform({1..30}, each {_ / 100}))',
        "format": {"Rate": "0%"},
    },
    "Status Class": {
        "columns": [("status_class", "string"), ("status_order", "int64"), ("status_color", "string")],
        "m": '#table(type table [status_class = text, status_order = Int64.Type, status_color = text], {'
             '{"Empty", 1, "#B2182B"}, {"Low", 2, "#F4A582"}, {"Balanced", 3, "#92C5DE"}, '
             '{"High", 4, "#4393C3"}, {"Full", 5, "#2166AC"}})',
        "sort": {"status_class": "status_order"},
        "hidden": {"status_order", "status_color"},
    },
}

# (from_table, from_col, to_table, to_col, active)
RELATIONSHIPS = [
    ("fact_trips_hourly", "date", "dim_date", "date", True),
    ("fact_trips_hourly", "hour", "dim_hour", "hour", True),
    ("fact_trips_hourly", "start_station_key", "dim_station", "station_key", True),
    ("fact_station_hourly", "date", "dim_date", "date", True),
    ("fact_station_hourly", "hour", "dim_hour", "hour", True),
    ("fact_station_hourly", "station_key", "dim_station", "station_key", True),
    ("fact_od_monthly", "start_station_key", "dim_station", "station_key", True),
    ("fact_od_monthly", "end_station_key", "dim_station", "station_key", False),
    ("fact_top_routes", "start_station_key", "dim_station", "station_key", True),
    ("fact_casual_repeat_od", "start_station_key", "dim_station", "station_key", True),
    ("fact_station_profile", "station_key", "dim_station", "station_key", True),
    ("fact_station_profile", "hour", "dim_hour", "hour", True),
    ("fact_customer_profile", "hour", "dim_hour", "hour", True),
    ("fact_station_live", "station_key", "dim_station", "station_key", True),
    ("fact_station_live", "status_class", "Status Class", "status_class", True),
    ("fact_station_status", "station_key", "dim_station", "station_key", True),
    ("fact_rebalance_list", "station_key", "dim_station", "station_key", True),
    ("fact_neighborhood_live", "nta_code", "dim_neighborhood", "nta_code", True),
    ("fact_weather_hourly", "date", "dim_date", "date", True),
    ("fact_weather_hourly", "hour", "dim_hour", "hour", True),
    ("dim_station", "nta_code", "dim_neighborhood", "nta_code", True),
]

# --------------------------------------------------------------------------- measures
# (name, display folder, format string, DAX, hidden)
FMT_INT, FMT_DEC1, FMT_DEC2, FMT_PCT, FMT_PCT_SIGNED = "#,0", "#,0.0", "#,0.00", "0.0%", "+0.0%;-0.0%;0.0%"
CITY = "REMOVEFILTERS(dim_neighborhood), REMOVEFILTERS(dim_station)"
UP, DOWN = "▲", "▼"
BLUE, RED, ORANGE, GREY = "#2166AC", "#B2182B", "#D6604D", "#5B6478"

M = []


def m(name, folder, fmt, dax, hidden=False):
    M.append((name, folder, fmt, dax.strip("\n"), hidden))


# ---- Trips -------------------------------------------------------------------------
m("Total Trips", "Trips", FMT_INT, "SUM ( fact_trips_hourly[trip_count] )")
m("Trips per Day", "Trips", FMT_INT, "DIVIDE ( [Total Trips], DISTINCTCOUNT ( fact_trips_hourly[date] ) )")
m("_Last Data Date", "Trips", "yyyy-mm-dd",
  "CALCULATE ( MAX ( fact_trips_hourly[date] ), REMOVEFILTERS ( dim_date ) )", hidden=True)
m("Trips Last 7D", "Trips", FMT_INT, """
VAR d = [_Last Data Date]
RETURN CALCULATE ( [Total Trips], REMOVEFILTERS ( dim_date ), DATESBETWEEN ( dim_date[date], d - 6, d ) )""")
m("Trips Prev 7D", "Trips", FMT_INT, """
VAR d = [_Last Data Date]
RETURN CALCULATE ( [Total Trips], REMOVEFILTERS ( dim_date ), DATESBETWEEN ( dim_date[date], d - 13, d - 7 ) )""")
m("Trips WoW %", "Trips", FMT_PCT_SIGNED, "DIVIDE ( [Trips Last 7D] - [Trips Prev 7D], [Trips Prev 7D] )")
m("Trips PY", "Trips", FMT_INT, "CALCULATE ( [Total Trips], SAMEPERIODLASTYEAR ( dim_date[date] ) )")
m("Trips YoY %", "Trips", FMT_PCT_SIGNED, "DIVIDE ( [Total Trips] - [Trips PY], [Trips PY] )")
m("Trips MTD", "Trips", FMT_INT, "TOTALMTD ( [Total Trips], dim_date[date] )")
m("Trips 7D Rolling Avg", "Trips", FMT_INT, """
VAR d = MAX ( dim_date[date] )
RETURN IF ( d <= [_Last Data Date],
    AVERAGEX ( DATESINPERIOD ( dim_date[date], d, -7, DAY ), [Total Trips] ) )""")
m("Classic Trips", "Trips", FMT_INT, 'CALCULATE ( [Total Trips], fact_trips_hourly[rideable_type] = "classic_bike" )')
m("E-bike Trips", "Trips", FMT_INT, 'CALCULATE ( [Total Trips], fact_trips_hourly[rideable_type] = "electric_bike" )')
m("Avg Trips per Hour", "Trips", FMT_INT, "DIVIDE ( [Total Trips], DISTINCTCOUNT ( fact_trips_hourly[date] ) )")
m("Weekday Avg Trips per Hour", "Trips", FMT_INT, "CALCULATE ( [Avg Trips per Hour], dim_date[is_weekend] = FALSE () )")
m("Weekend Avg Trips per Hour", "Trips", FMT_INT, "CALCULATE ( [Avg Trips per Hour], dim_date[is_weekend] = TRUE () )")

# ---- Normalization (Stations folder) -------------------------------------------------
m("Population", "Stations", FMT_INT, "SUM ( dim_neighborhood[population] )")
m("Docks", "Stations", FMT_INT, "CALCULATE ( SUM ( dim_station[capacity] ), dim_station[is_active] = TRUE () )")
m("Stations", "Stations", FMT_INT, "CALCULATE ( COUNTROWS ( dim_station ), dim_station[is_active] = TRUE () )")
m("Trips per 1k Residents", "Stations", FMT_DEC1, "DIVIDE ( [Total Trips], [Population] ) * 1000")
m("City Trips per 1k Residents", "Stations", FMT_DEC1, f"CALCULATE ( [Trips per 1k Residents], {CITY} )")
m("Trips per 1k Index", "Stations", "0", "DIVIDE ( [Trips per 1k Residents], [City Trips per 1k Residents] ) * 100")
m("Trips per Dock per Day", "Stations", FMT_DEC2, "DIVIDE ( [Trips per Day], [Docks] )")
m("City Trips per Dock per Day", "Stations", FMT_DEC2, f"CALCULATE ( [Trips per Dock per Day], {CITY} )")
m("Utilization Index", "Stations", "0", "DIVIDE ( [Trips per Dock per Day], [City Trips per Dock per Day] ) * 100")
m("Docks per 1k Residents", "Stations", FMT_DEC1, "DIVIDE ( [Docks], [Population] ) * 1000")
m("City Docks per 1k Residents", "Stations", FMT_DEC1,
  f"CALCULATE ( [Docks per 1k Residents], {CITY}, dim_neighborhood[in_service_area] = TRUE () )")
m("Trips Share of City", "Stations", FMT_PCT, f"DIVIDE ( [Total Trips], CALCULATE ( [Total Trips], {CITY} ) )")

# ---- Duration ---------------------------------------------------------------------------
m("Avg Duration (min)", "Duration", FMT_DEC1, "DIVIDE ( SUM ( fact_trips_hourly[total_duration_min] ), [Total Trips] )")
m("Avg Duration Last 7D", "Duration", FMT_DEC1, """
VAR d = [_Last Data Date]
RETURN CALCULATE ( [Avg Duration (min)], REMOVEFILTERS ( dim_date ), DATESBETWEEN ( dim_date[date], d - 6, d ) )""")
m("Avg Duration Prev 7D", "Duration", FMT_DEC1, """
VAR d = [_Last Data Date]
RETURN CALCULATE ( [Avg Duration (min)], REMOVEFILTERS ( dim_date ), DATESBETWEEN ( dim_date[date], d - 13, d - 7 ) )""")
m("Avg Duration WoW Change", "Duration", "+0.0;-0.0;0.0", "[Avg Duration Last 7D] - [Avg Duration Prev 7D]")
m("Avg Distance (km)", "Duration", FMT_DEC2,
  "DIVIDE ( SUM ( fact_trips_hourly[total_distance_km] ), SUM ( fact_trips_hourly[trips_with_distance] ) )")
m("Member Avg Duration", "Duration", FMT_DEC1, 'CALCULATE ( [Avg Duration (min)], fact_trips_hourly[member_casual] = "member" )')
m("Casual Avg Duration", "Duration", FMT_DEC1, 'CALCULATE ( [Avg Duration (min)], fact_trips_hourly[member_casual] = "casual" )')

# ---- Customers ----------------------------------------------------------------------------
m("Member Trips", "Customers", FMT_INT, 'CALCULATE ( [Total Trips], fact_trips_hourly[member_casual] = "member" )')
m("Casual Trips", "Customers", FMT_INT, 'CALCULATE ( [Total Trips], fact_trips_hourly[member_casual] = "casual" )')
m("Member Share %", "Customers", FMT_PCT, "DIVIDE ( [Member Trips], [Total Trips] )")
m("Casual Share %", "Customers", FMT_PCT, "DIVIDE ( [Casual Trips], [Total Trips] )")
m("E-bike Share %", "Customers", FMT_PCT, "DIVIDE ( [E-bike Trips], [Total Trips] )")
m("Member Share Last 7D", "Customers", FMT_PCT, """
VAR d = [_Last Data Date]
RETURN CALCULATE ( [Member Share %], REMOVEFILTERS ( dim_date ), DATESBETWEEN ( dim_date[date], d - 6, d ) )""")
m("Member Share Prev 7D", "Customers", FMT_PCT, """
VAR d = [_Last Data Date]
RETURN CALCULATE ( [Member Share %], REMOVEFILTERS ( dim_date ), DATESBETWEEN ( dim_date[date], d - 13, d - 7 ) )""")
m("E-bike Share Last 7D", "Customers", FMT_PCT, """
VAR d = [_Last Data Date]
RETURN CALCULATE ( [E-bike Share %], REMOVEFILTERS ( dim_date ), DATESBETWEEN ( dim_date[date], d - 6, d ) )""")
m("E-bike Share Prev 7D", "Customers", FMT_PCT, """
VAR d = [_Last Data Date]
RETURN CALCULATE ( [E-bike Share %], REMOVEFILTERS ( dim_date ), DATESBETWEEN ( dim_date[date], d - 13, d - 7 ) )""")
m("Casual Repeat Commute Trips", "Customers", FMT_INT, "SUM ( fact_casual_repeat_od[trips] )")
m("Conversion Targets (est. riders)", "Customers", FMT_INT, """
-- No rider id exists in the open data. Proxy: each origin-destination pair that a casual
-- rider used >= 3 times in one week on weekday commute hours counts as >= 1 commuter.
-- Lower-bound estimate, averaged over the weeks in the data.
AVERAGEX (
    VALUES ( fact_casual_repeat_od[week_start] ),
    CALCULATE ( COUNTROWS ( fact_casual_repeat_od ) )
)""")
m("Membership Price", "Customers", "$#,0", "SELECTEDVALUE ( 'Membership Price'[Price], 220 )")
m("Conversion Rate", "Customers", "0%", "SELECTEDVALUE ( 'Conversion Rate'[Rate], 0.1 )")
m("Revenue Opportunity", "Customers", "$#,0",
  "[Conversion Targets (est. riders)] * [Conversion Rate] * [Membership Price]")

# ---- Live Ops -----------------------------------------------------------------------------
m("Stations Reporting", "Live Ops", FMT_INT, "COUNTROWS ( fact_station_live )")
m("Empty Stations (now)", "Live Ops", FMT_INT,
  'CALCULATE ( COUNTROWS ( fact_station_live ), fact_station_live[status_class] = "Empty" ) + 0')
m("Full Stations (now)", "Live Ops", FMT_INT,
  'CALCULATE ( COUNTROWS ( fact_station_live ), fact_station_live[status_class] = "Full" ) + 0')
m("Balanced Stations (now)", "Live Ops", FMT_INT,
  'CALCULATE ( COUNTROWS ( fact_station_live ), fact_station_live[status_class] = "Balanced" ) + 0')
m("% Stations Balanced", "Live Ops", FMT_PCT, "DIVIDE ( [Balanced Stations (now)], [Stations Reporting] )")
m("% Balanced Target", "Live Ops", "0%", "0.85")
m("% Balanced 1h Ago", "Live Ops", FMT_PCT, """
DIVIDE (
    SUMX ( fact_neighborhood_live, fact_neighborhood_live[pct_balanced_1h_ago] * fact_neighborhood_live[stations_reporting] ),
    SUM ( fact_neighborhood_live[stations_reporting] )
)""")
m("% Empty Stations", "Live Ops", FMT_PCT, "DIVIDE ( [Empty Stations (now)], [Stations Reporting] )")
m("% Full Stations", "Live Ops", FMT_PCT, "DIVIDE ( [Full Stations (now)], [Stations Reporting] )")
m("Bikes Available", "Live Ops", FMT_INT, "SUM ( fact_station_live[num_bikes_available] )")
m("E-bikes Available", "Live Ops", FMT_INT, "SUM ( fact_station_live[num_ebikes_available] )")
m("Docks Available", "Live Ops", FMT_INT, "SUM ( fact_station_live[num_docks_available] )")
m("Bikes Change 1h", "Live Ops", "+#,0;-#,0;0", "SUM ( fact_neighborhood_live[bikes_change_1h] )")
m("Bikes per 1k Residents (live)", "Live Ops", FMT_DEC1, "DIVIDE ( [Bikes Available], [Population] ) * 1000")
m("Rebalance Backlog", "Live Ops", FMT_INT, """
COUNTROWS (
    FILTER (
        fact_station_live,
        fact_station_live[status_class] IN { "Empty", "Full" }
            || ( NOT ISBLANK ( fact_station_live[est_minutes_to_empty] ) && fact_station_live[est_minutes_to_empty] <= 60 )
            || ( NOT ISBLANK ( fact_station_live[est_minutes_to_full] ) && fact_station_live[est_minutes_to_full] <= 60 )
    )
) + 0""")
m("Bikes to Deliver", "Live Ops", FMT_INT,
  "SUMX ( FILTER ( fact_rebalance_list, fact_rebalance_list[bikes_to_target] > 0 ), fact_rebalance_list[bikes_to_target] ) + 0")
m("Bikes to Pick Up", "Live Ops", FMT_INT,
  "SUMX ( FILTER ( fact_rebalance_list, fact_rebalance_list[bikes_to_target] < 0 ), -fact_rebalance_list[bikes_to_target] ) + 0")
m("Station Status", "Live Ops", "", "SELECTEDVALUE ( fact_station_live[status_class] )")
m("Station Capacity", "Live Ops", FMT_INT, "MAX ( fact_station_live[capacity] )")
m("Station Bikes", "Live Ops", FMT_INT, "SUM ( fact_station_live[num_bikes_available] )")
m("Station Docks", "Live Ops", FMT_INT, "SUM ( fact_station_live[num_docks_available] )")
m("Station Min to Empty", "Live Ops", "0", "MIN ( fact_station_live[est_minutes_to_empty] )")
m("Station Priority", "Live Ops", "0", "MAX ( fact_station_live[rebalance_priority_score] )")
m("Status Color", "Live Ops", "", "SELECTEDVALUE ( 'Status Class'[status_color], \"#E6EAF0\" )")
m("Net Flow", "Live Ops", "+#,0;-#,0;0", "SUM ( fact_station_hourly[net_flow] )")
m("Departures", "Live Ops", FMT_INT, "SUM ( fact_station_hourly[departures] )")
m("Arrivals", "Live Ops", FMT_INT, "SUM ( fact_station_hourly[arrivals] )")
m("_Weekdays", "Live Ops", FMT_INT, f"""
CALCULATE ( DISTINCTCOUNT ( fact_station_hourly[date] ), dim_date[is_weekend] = FALSE (), {CITY} )""", hidden=True)
m("AM Net Flow per Weekday", "Live Ops", "+#,0.0;-#,0.0;0", """
DIVIDE ( CALCULATE ( [Net Flow], dim_hour[day_part] = "AM Peak", dim_date[is_weekend] = FALSE () ), [_Weekdays] )""")
m("PM Net Flow per Weekday", "Live Ops", "+#,0.0;-#,0.0;0", """
DIVIDE ( CALCULATE ( [Net Flow], dim_hour[day_part] = "PM Peak", dim_date[is_weekend] = FALSE () ), [_Weekdays] )""")

# ---- Map metric switcher -----------------------------------------------------------------
m("Map Color Value", "Live Ops", FMT_DEC2, """
SWITCH ( SELECTEDVALUE ( 'Map Metric'[metric_id], 1 ),
    1, [% Stations Balanced],
    2, [Bikes per 1k Residents (live)],
    3, [Trips per 1k Residents],
    4, [Trips WoW %],
    5, [Trips per Dock per Day] )""")
m("Map Color City", "Live Ops", FMT_DEC2, f"CALCULATE ( [Map Color Value], {CITY} )")
m("Map Color Index", "Live Ops", "0", """
-- every metric as an index vs the city (100 = city average) so one diverging scale fits all
VAR v = [Map Color Value]
VAR c = [Map Color City]
RETURN IF ( NOT ISBLANK ( v ),
    IF ( SELECTEDVALUE ( 'Map Metric'[metric_id], 1 ) = 4, DIVIDE ( 1 + v, 1 + c ) * 100, DIVIDE ( v, c ) * 100 ) )""")
m("Map Value Text", "Live Ops", "", """
VAR v = [Map Color Value]
RETURN SWITCH ( SELECTEDVALUE ( 'Map Metric'[metric_id], 1 ),
    1, FORMAT ( v, "0%" ) & " stations balanced",
    2, FORMAT ( v, "0.0" ) & " bikes per 1k residents",
    3, FORMAT ( v, "#,0" ) & " trips per 1k residents",
    4, FORMAT ( v, "+0.0%;-0.0%" ) & " trips week over week",
    5, FORMAT ( v, "0.00" ) & " trips per dock per day" )""")
m("Map Metric Label", "Live Ops", "", 'SELECTEDVALUE ( \'Map Metric\'[Metric], "Live balance" )')
m("NTA Color Index", "Live Ops", "0", """
-- evaluated per station row in the map dataset, but at the station's neighborhood level
VAR n = SELECTEDVALUE ( dim_station[nta_code] )
RETURN CALCULATE ( [Map Color Index], REMOVEFILTERS ( dim_station ), dim_neighborhood[nta_code] = n )""")
m("NTA Value Text", "Live Ops", "", """
VAR n = SELECTEDVALUE ( dim_station[nta_code] )
RETURN CALCULATE ( [Map Value Text], REMOVEFILTERS ( dim_station ), dim_neighborhood[nta_code] = n )""")

# ---- Data Quality --------------------------------------------------------------------------
m("Rows In (trips)", "Data Quality", FMT_INT, "MAX ( fact_trip_cleaning[rows_in] )")
m("Rows Removed", "Data Quality", FMT_INT,
  'CALCULATE ( SUM ( fact_trip_cleaning[rows_affected] ), fact_trip_cleaning[action] = "REMOVE" ) + 0')
m("Rows Flagged", "Data Quality", FMT_INT,
  'CALCULATE ( SUM ( fact_trip_cleaning[rows_affected] ), fact_trip_cleaning[action] = "FLAG" ) + 0')
m("Rows Fixed", "Data Quality", FMT_INT,
  'CALCULATE ( SUM ( fact_trip_cleaning[rows_affected] ), fact_trip_cleaning[action] = "FIX" ) + 0')
m("Rows Removed by Cleaning %", "Data Quality", "0.00%", "DIVIDE ( [Rows Removed], [Rows In (trips)] )")
m("Rows Affected", "Data Quality", FMT_INT, "SUM ( fact_trip_cleaning[rows_affected] )")
m("Live Rows Affected", "Data Quality", FMT_INT, "SUM ( fact_data_quality[rows_affected] )")
m("Unmatched Station IDs", "Data Quality", FMT_INT, "COUNTROWS ( fact_unmatched_stations ) + 0")
m("Unmatched Trips", "Data Quality", FMT_INT, "SUM ( fact_unmatched_stations[trips] ) + 0")
m("Pipeline Runs", "Data Quality", FMT_INT, "COUNTROWS ( fact_pipeline_runs )")
m("Pipeline Success Rate", "Data Quality", FMT_PCT,
  'DIVIDE ( CALCULATE ( COUNTROWS ( fact_pipeline_runs ), fact_pipeline_runs[status] = "success" ), [Pipeline Runs] )')
m("Live Snapshot (ET)", "Data Quality", "mmm d, yyyy h:nn AM/PM", "MAX ( fact_station_live[snapshot_utc_ny] )")
m("Data Freshness (min)", "Data Quality", FMT_INT,
  "DATEDIFF ( MAX ( fact_station_live[snapshot_utc] ), UTCNOW (), MINUTE )")
m("Snapshots Kept", "Data Quality", FMT_INT, "DISTINCTCOUNT ( fact_station_status[snapshot_utc] )")
m("Data As Of Text", "Data Quality", "", """
"Data as of " & FORMAT ( [Live Snapshot (ET)], "mmm d, yyyy h:nn AM/PM" ) & " ET  ·  live snapshot every 30 min  ·  trips "
    & FORMAT ( CALCULATE ( MIN ( fact_trips_hourly[date] ), REMOVEFILTERS () ), "mmm d" ) & " – "
    & FORMAT ( CALCULATE ( MAX ( fact_trips_hourly[date] ), REMOVEFILTERS () ), "mmm d, yyyy" )""")

# ---- Routes ---------------------------------------------------------------------------------
m("Route Trips", "Trips", FMT_INT, "SUM ( fact_top_routes[trips] )")
m("Route Trips per Day", "Trips", FMT_DEC1, "SUM ( fact_top_routes[trips_per_day] )")
m("Route Member Share", "Trips", FMT_PCT, "DIVIDE ( SUM ( fact_top_routes[member_trips] ), [Route Trips] )")
m("Top Route Rank", "Trips", "0", """
IF ( HASONEVALUE ( fact_top_routes[route] ),
    RANKX ( ALLSELECTED ( fact_top_routes[route] ), [Route Trips], , DESC, DENSE ) )""")

# ---- Weather -------------------------------------------------------------------------------
m("Trips vs Normal", "Trips", FMT_PCT, "AVERAGE ( fact_weather_hourly[trips_vs_normal] )")
m("Rain Impact %", "Trips", FMT_PCT_SIGNED,
  'CALCULATE ( [Trips vs Normal], fact_weather_hourly[weather_class] = "Rain" ) - 1')

# ---- colours for conditional formatting -------------------------------------------------
m("KPI Color Trips", "Trips", "", f'IF ( [Trips WoW %] >= 0, "{BLUE}", "{RED}" )')
m("KPI Color Member", "Customers", "", f'IF ( [Member Share Last 7D] >= [Member Share Prev 7D], "{BLUE}", "{RED}" )')
m("KPI Color Balanced", "Live Ops", "", f"""
SWITCH ( TRUE (), [% Stations Balanced] >= [% Balanced Target], "{BLUE}",
    [% Stations Balanced] >= 0.7, "{ORANGE}", "{RED}" )""")
m("KPI Color Neutral", "Trips", "", f'"{GREY}"')
m("KPI Color Freshness", "Data Quality", "", f'IF ( [Data Freshness (min)] <= 60, "{BLUE}", "{RED}" )')
m("Bar Color vs City (trips per 1k)", "Stations", "",
  f'IF ( [Trips per 1k Index] >= 100, "{BLUE}", IF ( [Trips per 1k Index] >= 70, "{ORANGE}", "{RED}" ) )')
m("Bar Color vs City (utilization)", "Stations", "",
  f'IF ( [Utilization Index] >= 150, "{RED}", IF ( [Utilization Index] >= 100, "{ORANGE}", "{BLUE}" ) )')
m("Heat Color", "Trips", "", """
-- 5-step blue scale relative to the busiest hour in the current view
VAR v = [Avg Trips per Hour]
VAR mx = MAXX ( CROSSJOIN ( ALLSELECTED ( dim_date[weekday_name] ), ALLSELECTED ( dim_hour[hour] ) ), [Avg Trips per Hour] )
VAR r = DIVIDE ( v, mx )
RETURN SWITCH ( TRUE (), ISBLANK ( v ), "#FFFFFF", r >= 0.8, "#2166AC", r >= 0.6, "#4393C3",
    r >= 0.4, "#92C5DE", r >= 0.2, "#D1E5F0", "#F3F5F8" )""")
m("Heat Font Color", "Trips", "", """
VAR r = DIVIDE ( [Avg Trips per Hour], MAXX ( CROSSJOIN ( ALLSELECTED ( dim_date[weekday_name] ), ALLSELECTED ( dim_hour[hour] ) ), [Avg Trips per Hour] ) )
RETURN IF ( r >= 0.6, "#FFFFFF", "#1F2A44" )""")
m("Action Color", "Stations", "", """
SWITCH ( [Recommended Action],
    "Rebalance now", "#FDDBC7", "Add docks", "#F4A582", "Expand / promote", "#D1E5F0", "#FFFFFF" )""")

# ---- KPI footers (text) -----------------------------------------------------------------------
m("KPI Footer Trips", "Trips", "", f"""
VAR w = [Trips WoW %]
RETURN IF ( w >= 0, "{UP} ", "{DOWN} " ) & FORMAT ( ABS ( w ), "0.0%" ) & " vs prev 7 days  ·  "
    & FORMAT ( [Trips per Day], "#,0" ) & "/day" """)
m("KPI Footer Duration", "Duration", "", f"""
VAR c = [Avg Duration WoW Change]
RETURN IF ( c >= 0, "{UP} ", "{DOWN} " ) & FORMAT ( ABS ( c ), "0.0" ) & " min vs prev 7 days  ·  "
    & FORMAT ( [Avg Distance (km)], "0.0" ) & " km avg" """)
m("KPI Footer Member", "Customers", "", f"""
VAR c = ( [Member Share Last 7D] - [Member Share Prev 7D] ) * 100
RETURN IF ( c >= 0, "{UP} ", "{DOWN} " ) & FORMAT ( ABS ( c ), "0.0" ) & " pp vs prev 7 days  ·  target 75%" """)
m("KPI Footer Balanced", "Live Ops", "", f"""
VAR c = ( [% Stations Balanced] - [% Balanced 1h Ago] ) * 100
RETURN "Target ≥ 85%  ·  " & IF ( ISBLANK ( [% Balanced 1h Ago] ), "no 1h history yet",
    IF ( c >= 0, "{UP} ", "{DOWN} " ) & FORMAT ( ABS ( c ), "0.0" ) & " pp vs 1h ago" )""")
m("KPI Footer Empty", "Live Ops", "", 'FORMAT ( [% Empty Stations], "0.0%" ) & " of stations  ·  target ≤ 5%"')
m("KPI Footer Full", "Live Ops", "", 'FORMAT ( [% Full Stations], "0.0%" ) & " of stations  ·  target ≤ 5%"')
m("KPI Footer Bikes", "Live Ops", "", f"""
VAR c = [Bikes Change 1h]
RETURN IF ( ISBLANK ( c ), "no 1h history yet", IF ( c >= 0, "{UP} ", "{DOWN} " ) & FORMAT ( ABS ( c ), "#,0" ) & " vs 1h ago" )
    & "  ·  " & FORMAT ( [E-bikes Available], "#,0" ) & " e-bikes" """)
m("KPI Footer Docks", "Live Ops", "", '"Backlog: " & FORMAT ( [Rebalance Backlog], "#,0" ) & " stations need a truck ≤ 60 min"')
m("KPI Color Empty", "Live Ops", "", f'IF ( [% Empty Stations] <= 0.05, "{BLUE}", "{RED}" )')
m("KPI Color Full", "Live Ops", "", f'IF ( [% Full Stations] <= 0.05, "{BLUE}", "{RED}" )')
m("KPI Footer Peak", "Trips", "", """
"weekday peak  ·  " & FORMAT ( DIVIDE ( [Weekday Avg Trips per Hour], [Weekend Avg Trips per Hour] ), "0.0" ) & "× weekend level" """)
m("Peak Hour Label", "Trips", "", """
VAR t = ADDCOLUMNS ( VALUES ( dim_hour[hour_label] ), "@v", CALCULATE ( [Weekday Avg Trips per Hour] ) )
RETURN MAXX ( TOPN ( 1, t, [@v], DESC ), dim_hour[hour_label] )""")
m("Peak Weekday Label", "Trips", "", """
VAR t = ADDCOLUMNS ( VALUES ( dim_date[weekday_name] ), "@v", CALCULATE ( [Trips per Day] ) )
RETURN MAXX ( TOPN ( 1, t, [@v], DESC ), dim_date[weekday_name] )""")
m("KPI Footer Weekday", "Trips", "", '"busiest weekday  ·  " & FORMAT ( MAXX ( VALUES ( dim_date[weekday_name] ), CALCULATE ( [Trips per Day] ) ), "#,0" ) & " trips/day"')
m("Weekend vs Weekday", "Trips", "0.00", "DIVIDE ( [Weekend Avg Trips per Hour], [Weekday Avg Trips per Hour] )")
m("KPI Footer Weekend", "Trips", "", '"weekend hourly demand vs weekday  ·  1.00 = equal"')
m("KPI Footer Rain", "Trips", "", '"trips in rainy hours vs normal for the same hour"')
m("KPI Footer Casual", "Customers", "", 'FORMAT ( [Casual Avg Duration], "0.0" ) & " min avg ride vs " & FORMAT ( [Member Avg Duration], "0.0" ) & " for members"')
m("KPI Footer Ebike", "Customers", "", f"""
VAR c = ( [E-bike Share Last 7D] - [E-bike Share Prev 7D] ) * 100
RETURN IF ( c >= 0, "{UP} ", "{DOWN} " ) & FORMAT ( ABS ( c ), "0.0" ) & " pp vs prev 7 days" """)
m("KPI Footer Revenue", "Customers", "", """
FORMAT ( [Conversion Targets (est. riders)], "#,0" ) & " riders × " & FORMAT ( [Conversion Rate], "0%" ) & " × " & FORMAT ( [Membership Price], "$#,0" ) & "/yr" """)
m("KPI Footer Routes", "Trips", "", '"busiest route  ·  " & FORMAT ( MAXX ( TOPN ( 1, fact_top_routes, fact_top_routes[trips] ), fact_top_routes[member_share] ), "0%" ) & " members"')
m("Top Route Trips per Day", "Trips", FMT_DEC1, "MAXX ( TOPN ( 1, fact_top_routes, fact_top_routes[trips] ), fact_top_routes[trips_per_day] )")
m("KPI Footer Distance", "Duration", "", '"straight-line start → end  ·  " & FORMAT ( [Avg Duration (min)], "0.0" ) & " min avg"')
m("KPI Footer Utilization", "Stations", "", '"city average  ·  target band 3–8 per dock"')
m("Under-served NTAs", "Stations", FMT_INT, """
COUNTROWS ( FILTER ( VALUES ( dim_neighborhood[nta_code] ),
    CALCULATE ( SUM ( dim_neighborhood[population] ) ) >= 20000
    && CALCULATE ( [Docks per 1k Residents] ) < 0.5 * [City Docks per 1k Residents] ) )""")
m("KPI Footer Underserved", "Stations", "", '"NTAs ≥ 20k residents with < 50% of city docks per 1k"')
m("KPI Footer Freshness", "Data Quality", "", '"minutes since last live snapshot  ·  target ≤ 60"')
m("KPI Footer Removed", "Data Quality", "", 'FORMAT ( [Rows Removed], "#,0" ) & " of " & FORMAT ( [Rows In (trips)], "#,0" ) & " trips  ·  tolerance ≤ 1%"')
m("KPI Footer Unmatched", "Data Quality", "", 'FORMAT ( [Unmatched Trips], "#,0" ) & " trips at retired / renamed stations"')
m("KPI Footer Runs", "Data Quality", "", 'FORMAT ( [Pipeline Runs], "#,0" ) & " runs logged  ·  " & FORMAT ( [Snapshots Kept], "#,0" ) & " live snapshots kept"')

# ---- Recommended action per neighborhood (table column) --------------------------------------
m("Recommended Action", "Stations", "", """
VAR pb = [% Stations Balanced]
VAR ui = [Utilization Index]
VAR ti = [Trips per 1k Index]
VAR pop = [Population]
VAR d1k = [Docks per 1k Residents]
RETURN SWITCH ( TRUE (),
    [Stations Reporting] >= 3 && pb < 0.6, "Rebalance now",
    ui >= 150 && d1k < [City Docks per 1k Residents], "Add docks",
    pop >= 20000 && ( ISBLANK ( ti ) || ti < 50 ), "Expand / promote",
    "Monitor" )""")

# ---- → Action captions (one per visual) -------------------------------------------------------
m("Action Map", "Live Ops", "", """
VAR t = FILTER (
    ADDCOLUMNS ( VALUES ( dim_neighborhood[nta_name] ),
        "@n", CALCULATE ( [Stations Reporting] ), "@pb", CALCULATE ( [% Stations Balanced] ),
        "@e", CALCULATE ( [Empty Stations (now)] ), "@f", CALCULATE ( [Full Stations (now)] ) ),
    [@n] >= 5 )
VAR w = TOPN ( 1, t, [@pb], ASC )
RETURN "→ Action: send a rebalancing crew to " & MAXX ( w, dim_neighborhood[nta_name] ) & " — only "
    & FORMAT ( MAXX ( w, [@pb] ), "0%" ) & " of stations balanced (" & MAXX ( w, [@e] ) & " empty, "
    & MAXX ( w, [@f] ) & " full) vs " & FORMAT ( [% Stations Balanced], "0%" ) & " city-wide" """)
m("Action Trend", "Trips", "", """
VAR w = [Trips WoW %]
RETURN "→ Action: trips " & FORMAT ( w, "+0.0%;-0.0%" ) & " week over week; e-bikes are "
    & FORMAT ( [E-bike Share %], "0%" ) & " of rides — "
    & IF ( w < 0, "check whether empty/full stations are suppressing demand on the weakest days",
        "add early-morning rebalancing shifts to keep pace with demand" )""")
m("Action Borough", "Stations", "", """
VAR t = FILTER ( ADDCOLUMNS ( VALUES ( dim_neighborhood[borough] ),
        "@i", CALCULATE ( [Trips per 1k Index] ), "@v", CALCULATE ( [Trips per 1k Residents] ),
        "@d", CALCULATE ( [Docks] ) ), [@d] > 0 && NOT ISBLANK ( [@i] ) )
VAR w = TOPN ( 1, t, [@i], ASC )
RETURN "→ Action: grow ridership in " & MAXX ( w, dim_neighborhood[borough] ) & " — "
    & FORMAT ( MAXX ( w, [@v] ), "#,0" ) & " trips per 1k residents (index " & FORMAT ( MAXX ( w, [@i] ), "0" )
    & " vs city 100); add stations near its densest neighborhoods" """)
m("Action Members", "Customers", "", """
"→ Action: target ≈" & FORMAT ( [Conversion Targets (est. riders)], "#,0" )
    & " casual commuters (≥3 repeat trips/week) with a membership offer — est. "
    & FORMAT ( [Revenue Opportunity], "$#,0" ) & "/yr at " & FORMAT ( [Conversion Rate], "0%" ) & " conversion" """)
m("Action Neighborhoods", "Stations", "", """
VAR t = ADDCOLUMNS ( VALUES ( dim_neighborhood[nta_code] ), "@a", [Recommended Action] )
RETURN "→ Action: rebalance " & COUNTROWS ( FILTER ( t, [@a] = "Rebalance now" ) ) & " NTAs now  ·  add docks in "
    & COUNTROWS ( FILTER ( t, [@a] = "Add docks" ) ) & "  ·  " & COUNTROWS ( FILTER ( t, [@a] = "Expand / promote" ) )
    & " expansion candidates" """)
m("Action Dispatch", "Live Ops", "", """
VAR top1 = TOPN ( 1, fact_rebalance_list, fact_rebalance_list[rebalance_priority_score], DESC, fact_rebalance_list[priority_rank], ASC )
RETURN "→ Action: dispatch trucks to the top " & COUNTROWS ( fact_rebalance_list ) & " stations — deliver "
    & FORMAT ( [Bikes to Deliver], "#,0" ) & " bikes, pick up " & FORMAT ( [Bikes to Pick Up], "#,0" )
    & "; first stop: " & MAXX ( top1, fact_rebalance_list[name] ) & " (" & MAXX ( top1, fact_rebalance_list[status_class] ) & ")" """)
m("Action Live Map", "Live Ops", "", """
VAR t = ADDCOLUMNS ( VALUES ( dim_station[borough] ), "@e", CALCULATE ( [Empty Stations (now)] ) )
VAR w = TOPN ( 1, t, [@e], DESC )
RETURN "→ Action: " & [Empty Stations (now)] & " empty / " & [Full Stations (now)] & " full now — most empties in "
    & MAXX ( w, dim_station[borough] ) & " (" & MAXX ( w, [@e] ) & "); route the first truck there" """)
m("Action Heatmap", "Trips", "", """
"→ Action: pre-position bikes before " & [Peak Hour Label] & " on " & [Peak Weekday Label]
    & "s — the busiest hour averages " & FORMAT ( MAXX ( ADDCOLUMNS ( CROSSJOIN ( VALUES ( dim_date[weekday_name] ), VALUES ( dim_hour[hour] ) ), "@v", CALCULATE ( [Avg Trips per Hour] ) ), [@v] ), "#,0" )
    & " trips city-wide" """)
m("Action Hourly", "Trips", "", """
"→ Action: weekday peak at " & [Peak Hour Label] & " is " & FORMAT ( DIVIDE (
    MAXX ( VALUES ( dim_hour[hour] ), CALCULATE ( [Weekday Avg Trips per Hour] ) ),
    MAXX ( VALUES ( dim_hour[hour] ), CALCULATE ( [Weekend Avg Trips per Hour] ) ) ), "0.0" )
    & "× the weekend peak — schedule crews 60 min before it" """)
m("Action Net Flow", "Live Ops", "", """
VAR t = FILTER ( ADDCOLUMNS ( VALUES ( dim_neighborhood[nta_name] ), "@f", CALCULATE ( [AM Net Flow per Weekday] ) ), NOT ISBLANK ( [@f] ) )
VAR w = TOPN ( 1, t, [@f], ASC )
RETURN "→ Action: pre-position " & FORMAT ( -MAXX ( w, [@f] ), "#,0" ) & " bikes in " & MAXX ( w, dim_neighborhood[nta_name] )
    & " before 7 AM (weekday morning drain)" """)
m("Action Weather", "Trips", "", """
"→ Action: rain cuts trips to " & FORMAT ( 1 + [Rain Impact %], "0%" )
    & " of normal for the same hour — scale down crews and shift to charging on rainy forecasts" """)
m("Action Duration", "Customers", "", """
"→ Action: casual rides are " & FORMAT ( DIVIDE ( [Casual Avg Duration], [Member Avg Duration] ), "0.0" )
    & "× longer than member rides — sell day passes to leisure riders, memberships to commuters" """)
m("Action Time of Day", "Customers", "", """
VAR t = ADDCOLUMNS ( VALUES ( dim_hour[hour_label] ), "@c", CALCULATE ( [Casual Share %] ) )
VAR w = TOPN ( 1, t, [@c], DESC )
RETURN "→ Action: casual share peaks at " & MAXX ( w, dim_hour[hour_label] ) & " (" & FORMAT ( MAXX ( w, [@c] ), "0%" )
    & ") — time in-app membership offers to that hour" """)
m("Action Ebike", "Customers", "", """
"→ Action: e-bikes are " & FORMAT ( [E-bike Share %], "0%" ) & " of trips — prioritise e-bike charging and swaps at the busiest stations" """)
m("Action Conversion", "Customers", "", """
VAR t = TOPN ( 1, ADDCOLUMNS ( VALUES ( dim_station[name] ), "@t", CALCULATE ( [Casual Repeat Commute Trips] ) ), [@t], DESC )
RETURN "→ Action: start the membership campaign at " & MAXX ( t, dim_station[name] ) & " ("
    & FORMAT ( MAXX ( t, [@t] ), "#,0" ) & " repeat casual commute trips)" """)
m("Action Routes", "Trips", "", """
VAR t = TOPN ( 1, fact_top_routes, fact_top_routes[trips], DESC )
RETURN "→ Action: busiest route " & MAXX ( t, fact_top_routes[route] ) & " carries " & FORMAT ( MAXX ( t, fact_top_routes[trips_per_day] ), "0" )
    & " trips/day (" & FORMAT ( MAXX ( t, fact_top_routes[member_share] ), "0%" ) & " members) — keep docks free at both ends" """)
m("Action Utilization", "Stations", "", """
VAR c = [City Trips per Dock per Day]
VAR t = FILTER ( ADDCOLUMNS ( VALUES ( dim_station[name] ), "@u", CALCULATE ( [Trips per Dock per Day] ), "@cap", CALCULATE ( [Docks] ) ), [@cap] > 0 )
VAR w = TOPN ( 1, t, [@u], DESC )
VAR ratio = DIVIDE ( MAXX ( w, [@u] ), c )
VAR add = ROUNDUP ( MAXX ( w, [@cap] ) * ( ratio / 1.5 - 1 ), 0 )
RETURN "→ Action: add " & add & " docks at " & MAXX ( w, dim_station[name] ) & " (utilization " & FORMAT ( ratio, "0.0" )
    & "× city avg) to bring it to 1.5× city" """)
m("Action Expansion", "Stations", "", """
VAR c = [City Docks per 1k Residents]
VAR t = FILTER ( ADDCOLUMNS ( VALUES ( dim_neighborhood[nta_name] ),
        "@p", CALCULATE ( [Population] ), "@d", CALCULATE ( [Docks per 1k Residents] ) ),
        [@p] >= 20000 )
VAR w = TOPN ( 1, t, [@d], ASC, [@p], DESC )
RETURN "→ Action: add stations in " & MAXX ( w, dim_neighborhood[nta_name] ) & " — " & FORMAT ( MAXX ( w, [@p] ), "#,0" )
    & " residents, " & FORMAT ( MAXX ( w, [@d] ), "0.0" ) & " docks per 1k vs city " & FORMAT ( c, "0.0" ) """)
m("Action Data Quality", "Data Quality", "", """
"→ Action: map " & [Unmatched Station IDs] & " unmatched station IDs (" & FORMAT ( [Unmatched Trips], "#,0" )
    & " trips) to current GBFS stations; " & FORMAT ( [Rows Removed by Cleaning %], "0.00%" ) & " of rows removed — "
    & IF ( [Rows Removed by Cleaning %] <= 0.01, "within", "above" ) & " the 1% tolerance" """)
m("Action Pipeline", "Data Quality", "", """
"→ Action: " & IF ( [Data Freshness (min)] > 60, "live data is stale (" & [Data Freshness (min)] & " min) — check the GitHub Actions run",
    "pipeline healthy: " & FORMAT ( [Pipeline Success Rate], "0%" ) & " of runs succeeded" )""")
m("Action Station", "Live Ops", "", """
VAR b = SELECTEDVALUE ( fact_station_live[bikes_to_target] )
RETURN "→ Action: " & SELECTEDVALUE ( dim_station[name], "select a station" ) & " is " & [Station Status]
    & IF ( ISBLANK ( b ), "", IF ( b > 0, " — deliver " & b & " bikes", " — pick up " & -b & " bikes" ) )
    & IF ( ISBLANK ( [Station Min to Empty] ), "", "; empty in ≈" & FORMAT ( [Station Min to Empty], "0" ) & " min" )""")
m("Insights Overview", "Live Ops", "", """
[Empty Stations (now)] + [Full Stations (now)] & " stations need rebalancing now; top priority: "
    & MAXX ( TOPN ( 1, fact_rebalance_list, fact_rebalance_list[rebalance_priority_score], DESC, fact_rebalance_list[priority_rank], ASC ), fact_rebalance_list[name] )
    & ".  Trips " & FORMAT ( [Trips WoW %], "+0.0%;-0.0%" ) & " WoW.  Members ride " & FORMAT ( [Member Share %], "0%" ) & " of trips." """)

# ---- tooltip page -----------------------------------------------------------------------------
m("Tooltip Rank", "Stations", "", """
VAR v = [% Stations Balanced]
VAR r = RANKX ( FILTER ( ALL ( dim_neighborhood[nta_name] ), CALCULATE ( [Stations Reporting] ) >= 3 ), CALCULATE ( [% Stations Balanced] ), v, DESC, DENSE )
RETURN IF ( ISBLANK ( v ), "no live stations", "#" & r & " of " & COUNTROWS ( FILTER ( ALL ( dim_neighborhood[nta_name] ), CALCULATE ( [Stations Reporting] ) >= 3 ) ) & " NTAs by balance" )""")
