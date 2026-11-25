"""Generate the Power BI Project (PBIP): TMDL semantic model + PBIR report.

    .venv\\Scripts\\python powerbi\\tools\\build_pbip.py

Re-running overwrites powerbi/CitiBike.SemanticModel and powerbi/CitiBike.Report
(do layout tweaks here, not by hand, so the project stays reproducible).
Column lists come from the Parquet files in data/clean.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
import model_def as MD  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PBI = ROOT / "powerbi"
CLEAN = ROOT / "data" / "clean"
NAME = "CitiBike"
SM = PBI / f"{NAME}.SemanticModel"
RP = PBI / f"{NAME}.Report"
NS = uuid.UUID("6f1c2a52-6b0e-4f7e-9b77-3c1d1b0a5e21")

SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"
V_VISUAL, V_PAGE, V_REPORT, V_BOOKMARK = "2.4.0", "2.0.0", "3.0.0", "1.4.0"
DENEB = "deneb7E15AEF80B9E4D4F8E12924291ECE89A"
NAVY, BLUE, GREY, BG = "#1F2A44", "#2F6FDE", "#5B6478", "#F3F5F8"


def clean_dir(path: Path):
    """Remove generated content but keep Power BI Desktop's local cache (.pbi: data cache, settings)."""
    if not path.exists():
        return
    for child in path.iterdir():
        if child.name == ".pbi":
            continue
        shutil.rmtree(child) if child.is_dir() else child.unlink()


def lt(name: str) -> str:
    return str(uuid.uuid5(NS, name))


def repo_base_url() -> str:
    url = subprocess.run(["git", "-C", str(ROOT), "remote", "get-url", "origin"], capture_output=True, text=True).stdout.strip()
    m = re.search(r"github\.com[:/](.+?)/(.+?)(\.git)?$", url)
    owner, repo = (m.group(1), m.group(2)) if m else ("OWNER", "citibike-bi")
    return f"https://raw.githubusercontent.com/{owner}/{repo}/data/"


# =============================================================================== TMDL
def q(name: str) -> str:
    return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) else "'" + name.replace("'", "''") + "'"


TYPE_MAP = {  # polars -> (tmdl dataType, M type, default format)
    "String": ("string", "type text", None),
    "Int64": ("int64", "Int64.Type", "#,0"),
    "Float64": ("double", "type number", "#,0.00"),
    "Boolean": ("boolean", "type logical", None),
    "Date": ("dateTime", "type date", "yyyy-mm-dd"),
}


def col_type(dtype) -> tuple[str, str, str | None, bool]:
    """Returns tmdl type, M type, format, is_utc_datetime."""
    s = str(dtype)
    if s.startswith("Datetime"):
        return "dateTime", "type datetime", "yyyy-mm-dd hh:nn", "UTC" in s
    key = s.split("(")[0]
    if key in ("Int8", "Int16", "Int32", "UInt8", "UInt16", "UInt32", "UInt64"):
        key = "Int64"
    if key == "Float32":
        key = "Float64"
    t = TYPE_MAP[key]
    return t[0], t[1], t[2], False


def m_block(lines: list[str], indent: int) -> str:
    pad = "\t" * indent
    return "\n".join(pad + ln if ln else "" for ln in lines)


def table_tmdl(table: str, file: str, is_fact: bool) -> str:
    schema = pl.read_parquet_schema(CLEAN / f"{file}.parquet")
    out = [f"table {q(table)}", f"\tlineageTag: {lt(table)}"]
    if table == "dim_date":
        out.insert(1, "\tdataCategory: Time")
    out.append("")
    types, utc_cols = [], []
    for c, dt in schema.items():
        tm, mt, fmt, is_utc = col_type(dt)
        types.append(f'{{"{c}", {mt}}}')
        if is_utc:
            utc_cols.append(c)
        hidden = (c in MD.HIDDEN_COLUMNS or c.endswith(MD.HIDDEN_COLUMN_SUFFIXES)) and not c.endswith("_ny")
        hidden = MD.VISIBLE_OVERRIDES.get((table, c), hidden)
        if c in MD.PERCENT_COLUMNS:
            fmt = "0.0%"
        if c in ("hour", "month", "year", "quarter", "weekday_num", "weekday", "month_key", "priority_rank",
                 "route_rank", "metric_id", "day_part_order", "station_key", "date_key"):
            fmt = "0"
        out.append(f"\tcolumn {q(c)}")
        out.append(f"\t\tdataType: {tm}")
        if table == "dim_date" and c == "date":
            out.append("\t\tisKey")
        if fmt:
            out.append(f"\t\tformatString: {fmt}")
        if hidden:
            out.append("\t\tisHidden")
        out.append(f"\t\tlineageTag: {lt(table + '.' + c)}")
        out.append("\t\tsummarizeBy: none")
        out.append(f"\t\tsourceColumn: {c}")
        sb = MD.SORT_BY.get((table, c))
        if sb:
            out.append(f"\t\tsortByColumn: {q(sb)}")
        if c in ("lat", "start_lat", "end_lat"):
            out.append("\t\tdataCategory: Latitude")
        if c in ("lon", "start_lon", "end_lon"):
            out.append("\t\tdataCategory: Longitude")
        out.append("")
        out.append("\t\tannotation SummarizationSetBy = Automatic")
        if str(dt) == "Date":
            out.append("")
            out.append("\t\tannotation UnderlyingDateTimeDataType = Date")
        out.append("")

    m = ["let", f'    Source = fnRead("{file}"),']
    step = "Source"
    if utc_cols:
        conv = ", ".join(
            f'{{"{c}", each if _ = null then null else if Value.Is(_, type datetimezone) '
            f'then DateTimeZone.RemoveZone(DateTimeZone.ToUtc(_)) else _, type datetime}}' for c in utc_cols)
        m.append(f"    Utc = Table.TransformColumns({step}, {{{conv}}}),")
        step = "Utc"
    m.append(f"    Typed = Table.TransformColumnTypes({step}, {{{', '.join(types)}}})")
    m += ["in", "    Typed"]
    out += [f"\tpartition {q(table)} = m", "\t\tmode: import", "\t\tsource =", m_block(m, 4), ""]
    out += ["\tannotation PBI_ResultType = Table", ""]
    return "\n".join(out)


def helper_tmdl(table: str, spec: dict) -> str:
    out = [f"table {q(table)}", f"\tlineageTag: {lt(table)}", ""]
    for c, tm in spec["columns"]:
        out.append(f"\tcolumn {q(c)}")
        out.append(f"\t\tdataType: {tm}")
        fmt = spec.get("format", {}).get(c) or ("0" if tm == "int64" else None)
        if fmt:
            out.append(f"\t\tformatString: {fmt}")
        if c in spec.get("hidden", set()):
            out.append("\t\tisHidden")
        out.append(f"\t\tlineageTag: {lt(table + '.' + c)}")
        out.append("\t\tsummarizeBy: none")
        out.append(f"\t\tsourceColumn: {c}")
        if c in spec.get("sort", {}):
            out.append(f"\t\tsortByColumn: {q(spec['sort'][c])}")
        out += ["", "\t\tannotation SummarizationSetBy = Automatic", ""]
    out += [f"\tpartition {q(table)} = m", "\t\tmode: import", "\t\tsource =",
            m_block(["let", f"    Source = {spec['m']}", "in", "    Source"], 4), "",
            "\tannotation PBI_ResultType = Table", ""]
    return "\n".join(out)


def measures_tmdl() -> str:
    out = ["table _Measures", f"\tlineageTag: {lt('Measures')}", ""]
    for name, folder, fmt, dax, hidden in MD.M:
        lines = dax.split("\n")
        if len(lines) == 1:
            out.append(f"\tmeasure {q(name)} = {lines[0]}")
        else:
            out.append(f"\tmeasure {q(name)} = ```")
            out += ["\t\t\t" + ln for ln in lines]
            out.append("\t\t\t```")
        if fmt:
            out.append(f"\t\tformatString: {fmt}")
        if hidden:
            out.append("\t\tisHidden")
        out.append(f"\t\tdisplayFolder: {folder}")
        out.append(f"\t\tlineageTag: {lt('measure.' + name)}")
        out.append("")
    out += ["\tcolumn Dummy", "\t\tdataType: string", "\t\tisHidden", f"\t\tlineageTag: {lt('Measures.Dummy')}",
            "\t\tsummarizeBy: none", "\t\tsourceColumn: Dummy", "", "\t\tannotation SummarizationSetBy = Automatic", "",
            "\tpartition _Measures = m", "\t\tmode: import", "\t\tsource =",
            m_block(["let", '    Source = #table(type table [Dummy = text], {})', "in", "    Source"], 4), "",
            "\tannotation PBI_ResultType = Table", ""]
    return "\n".join(out)


def build_model(base_url: str) -> list[str]:
    clean_dir(SM)
    (SM / "definition" / "tables").mkdir(parents=True, exist_ok=True)
    (SM / "definition.pbism").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
        "version": "4.2", "settings": {}}, indent=2), encoding="utf-8")
    d = SM / "definition"
    (d / "database.tmdl").write_text("database\n\tcompatibilityLevel: 1567\n\n", encoding="utf-8")

    tables = []
    for t, f in {**MD.DIM_TABLES, **MD.FACT_TABLES}.items():
        (d / "tables" / f"{t}.tmdl").write_text(table_tmdl(t, f, t.startswith("fact")), encoding="utf-8")
        tables.append(t)
    for t, spec in MD.HELPER_TABLES.items():
        (d / "tables" / f"{t}.tmdl").write_text(helper_tmdl(t, spec), encoding="utf-8")
        tables.append(t)
    (d / "tables" / "_Measures.tmdl").write_text(measures_tmdl(), encoding="utf-8")
    tables.append("_Measures")

    fn = [
        "(name as text) as table =>",
        "let",
        "    // Parquet first (typed, compact); CSV fallback if Parquet cannot be read",
        '    Parquet = try Parquet.Document(Binary.Buffer(Web.Contents(BaseUrl, [RelativePath = "data/clean/" & name & ".parquet"]))),',
        "    Result = if Parquet[HasError] = false then Parquet[Value] else",
        '        Table.PromoteHeaders(Csv.Document(Web.Contents(BaseUrl, [RelativePath = "data/clean/" & name & ".csv"]),',
        '            [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]), [PromoteAllScalars = true])',
        "in",
        "    Result",
    ]
    expr = [
        f'expression BaseUrl = "{base_url}" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]',
        f"\tlineageTag: {lt('BaseUrl')}", "", "\tannotation PBI_ResultType = Text", "",
        "expression fnRead =", m_block(fn, 2),
        f"\tlineageTag: {lt('fnRead')}", "", "\tannotation PBI_ResultType = Function", "",
    ]
    (d / "expressions.tmdl").write_text("\n".join(expr), encoding="utf-8")

    rel = []
    for a, ac, b, bc, active in MD.RELATIONSHIPS:
        rel += [f"relationship {lt(f'rel.{a}.{ac}.{b}.{bc}')}"]
        if not active:
            rel.append("\tisActive: false")
        rel += [f"\tfromColumn: {q(a)}.{q(ac)}", f"\ttoColumn: {q(b)}.{q(bc)}", ""]
    (d / "relationships.tmdl").write_text("\n".join(rel), encoding="utf-8")

    order = json.dumps(["BaseUrl", "fnRead"] + tables)
    model = ["model Model", "\tculture: en-US", "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
             "\tdiscourageImplicitMeasures", "\tsourceQueryCulture: en-US", "\tdataAccessOptions",
             "\t\tlegacyRedirects", "\t\treturnErrorValuesAsNull", "",
             f"annotation PBI_QueryOrder = {order}", "", "annotation __PBI_TimeIntelligenceEnabled = 0", ""]
    model += [f"ref table {q(t)}" for t in tables] + [""]
    (d / "model.tmdl").write_text("\n".join(model), encoding="utf-8")
    return tables


# =============================================================================== PBIR helpers
def L(v):
    if isinstance(v, bool):
        s = "true" if v else "false"
    elif isinstance(v, (int, float)):
        s = f"{v}D"
    else:
        s = "'" + str(v).replace("'", "''") + "'"
    return {"expr": {"Literal": {"Value": s}}}


def MEAS(name, entity="_Measures"):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": name}}


def COL(table, col):
    return {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": col}}


def C(hexcolor):
    return {"solid": {"color": L(hexcolor)}}


def CM(measure):
    return {"solid": {"color": {"expr": MEAS(measure)}}}


def F(ref: str):
    """'Measures[Total Trips]' or 'dim_station[name]' -> (field, queryRef, nativeQueryRef)."""
    t, c = ref[:-1].split("[", 1)
    if t == "Measures":  # report shorthand; the model table is "_Measures" ("Measures" is reserved)
        t = "_Measures"
    field = MEAS(c, t) if t == "_Measures" else COL(t, c)
    return field, f"{t}.{c}", c


def props(**kw):
    return [{"properties": {k: (v if isinstance(v, dict) else L(v)) for k, v in kw.items()}}]


class Page:
    def __init__(self, name, display, width=1280, height=720, ptype="Default", hidden=False):
        self.name, self.display, self.w, self.h = name, display, width, height
        self.ptype, self.hidden = ptype, hidden
        self.visuals: list[dict] = []
        self.z = 1000
        self.extra: dict = {}

    def add(self, vname, vtype, x, y, w, h, roles=None, title=None, subtitle=None, objects=None, vco=None,
            sort=None, tooltip_page=None, parent=None, hidden=False):
        self.z += 100
        v: dict = {"visualType": vtype}
        if roles:
            qs = {}
            for role, refs in roles.items():
                projs = []
                for r in refs:
                    r, _, disp = r.partition("|")
                    fld, qr, nq = F(r)
                    pr = {"field": fld, "queryRef": qr, "nativeQueryRef": nq}
                    if disp:
                        pr["displayName"] = disp
                    projs.append(pr)
                qs[role] = {"projections": projs}
            v["query"] = {"queryState": qs}
            if sort:
                fld, _, _ = F(sort[0].partition("|")[0])
                v["query"]["sortDefinition"] = {"sort": [{"field": fld, "direction": sort[1]}], "isDefaultSort": False}
        if objects:
            v["objects"] = objects
        vc = dict(vco or {})
        if title is not None:
            vc.setdefault("title", props(show=True, text=title, fontSize=12, titleWrap=True, fontFamily="Segoe UI Semibold"))
        elif vtype not in ("shape", "textbox", "actionButton", "image"):
            vc.setdefault("title", props(show=False))
        if subtitle:
            vc["subTitle"] = [{"properties": {"show": L(True), "text": {"expr": MEAS(subtitle)}, "titleWrap": L(True),
                                             "fontSize": L(10), "fontColor": C(BLUE), "fontFamily": L("Segoe UI Semibold")}}]
        if tooltip_page:
            vc["visualTooltip"] = props(show=True, type="ReportPage", section=tooltip_page)
        if vc:
            v["visualContainerObjects"] = vc
        v["drillFilterOtherVisuals"] = True
        doc = {"$schema": f"{SCHEMA}/visualContainer/{V_VISUAL}/schema.json", "name": vname,
               "position": {"x": x, "y": y, "z": self.z, "width": w, "height": h, "tabOrder": self.z}, "visual": v}
        if parent:
            doc["parentGroupName"] = parent
        if hidden:
            doc["isHidden"] = True
        self.visuals.append(doc)
        return doc

    def group(self, gname, display, x, y, w, h, hidden=False):
        self.z += 100
        doc = {"$schema": f"{SCHEMA}/visualContainer/{V_VISUAL}/schema.json", "name": gname,
               "position": {"x": x, "y": y, "z": self.z, "width": w, "height": h, "tabOrder": self.z},
               "visualGroup": {"displayName": display, "groupMode": "ScaleMode"}}
        if hidden:
            doc["isHidden"] = True
        self.visuals.append(doc)


# ------------------------------------------------------------------------ building blocks
# Buttons/shapes ("new" visuals) need state selectors: {"id": "default"} / "hover" / "selected";
# properties without a selector only carry the show/hide switch.
def LI(v: int):
    return {"expr": {"Literal": {"Value": f"{int(v)}L"}}}


def DEF(**kw):
    return {"properties": {k: (v if isinstance(v, dict) else L(v)) for k, v in kw.items()}, "selector": {"id": "default"}}


def STATE(state, **kw):
    return {"properties": {k: (v if isinstance(v, dict) else L(v)) for k, v in kw.items()}, "selector": {"id": state}}


def no_chrome():
    return {"title": props(show=False), "background": props(show=False), "dropShadow": props(show=False),
            "border": props(show=False), "visualHeader": props(show=False),
            "padding": props(top=0, bottom=0, left=0, right=0)}


def textbox(p: Page, vname, x, y, w, h, text, size=11, color=NAVY, bold=False, font="Segoe UI", parent=None,
            hidden=False, center=False):
    style = {"fontFamily": font, "fontSize": f"{size}pt", "color": color}
    if bold:
        style["fontWeight"] = "bold"
    para = {"textRuns": [{"value": text, "textStyle": style}]}
    if center:
        para["horizontalTextAlignment"] = "center"
    p.add(vname, "textbox", x, y, w, h, parent=parent, hidden=hidden,
          objects={"general": [{"properties": {"paragraphs": [para]}}]}, vco=no_chrome())


def shape(p: Page, vname, x, y, w, h, fill, radius=0, parent=None, hidden=False, outline=None, shadow=False):
    out_props = {"show": L(bool(outline))}
    if outline:
        out_props.update(lineColor=C(outline), weight=LI(1))
    objs = {"shape": [{"properties": {"tileShape": L("rectangleRounded" if radius else "rectangle")}},
                      DEF(roundEdge=LI(radius))],
            "fill": [{"properties": {"show": L(True)}}, DEF(fillColor=C(fill), transparency=L(0))],
            "outline": [{"properties": {"show": L(bool(outline))}}, {"properties": out_props, "selector": {"id": "default"}}]}
    vco = no_chrome()
    if shadow:
        vco["dropShadow"] = props(show=True, preset="Custom", color=C(NAVY), transparency=90, shadowBlur=12,
                                  shadowDistance=2, shadowSpread=0, angle=90, position="Outer")
    p.add(vname, "shape", x, y, w, h, parent=parent, hidden=hidden, objects=objs, vco=vco)


def button(p: Page, vname, x, y, w, h, text, link: dict, *, color=GREY, hover_color=NAVY, fill=None,
           hover_fill="#F3F5F8", outline=None, bold=False, size=10, align="left", margin=14, radius=18,
           parent=None, hidden=False):
    out_props = {"show": L(bool(outline))}
    if outline:
        out_props.update(lineColor=C(outline), weight=LI(1))
    objs = {
        "icon": [{"properties": {"show": L(False)}}],
        "text": [{"properties": {"show": L(True)}},
                 DEF(text=text, fontColor=C(color), fontSize=L(size), bold=L(bold), fontFamily="Segoe UI",
                     horizontalAlignment=align, leftMargin=LI(margin)),
                 STATE("hover", fontColor=C(hover_color))],
        "fill": [{"properties": {"show": L(True)}},
                 DEF(fillColor=C(fill or "#FFFFFF"), transparency=L(0 if fill else 100)),
                 STATE("hover", fillColor=C(hover_fill), transparency=L(0))],
        "outline": [{"properties": {"show": L(bool(outline))}}, {"properties": out_props, "selector": {"id": "default"}}],
        "shape": [DEF(roundEdge=LI(radius))],
    }
    vco = no_chrome()
    vco["visualLink"] = [{"properties": {"show": L(True), **{k: L(v) for k, v in link.items()}}}]
    p.add(vname, "actionButton", x, y, w, h, objects=objs, vco=vco, parent=parent, hidden=hidden)


def dyn_text(p: Page, vname, x, y, w, h, measure, size=9, color=GREY, color_measure=None, bold=False):
    """Left-aligned dynamic text: a transparent shape whose title is bound to a measure."""
    t = {"show": L(True), "text": {"expr": MEAS(measure)}, "fontSize": L(size), "alignment": L("left"),
         "fontColor": CM(color_measure) if color_measure else C(color),
         "fontFamily": L("Segoe UI Semibold" if bold else "Segoe UI"), "titleWrap": L(True),
         }
    vco = no_chrome()
    vco["title"] = [{"properties": t}]
    p.add(vname, "shape", x, y, w, h, vco=vco,
          objects={"shape": [{"properties": {"tileShape": L("rectangle")}}],
                   "fill": [{"properties": {"show": L(False)}}, DEF(transparency=L(100))],
                   "outline": [{"properties": {"show": L(False)}}, DEF(show=L(False))]})


def kpi(p: Page, key, x, y, w, label, value_measure, footer_measure, color_measure="KPI Color Neutral", h=104):
    shape(p, f"{key}_bg", x, y, w, h, "#FFFFFF", radius=10, shadow=True)
    shape(p, f"{key}_accent", x + 14, y, 36, 4, BLUE, radius=2)
    textbox(p, f"{key}_label", x + 12, y + 12, w - 24, 20, label.upper(), size=9, color=GREY, font="Segoe UI Semibold")
    p.add(f"{key}_value", "card", x + 4, y + 30, w - 8, 44, roles={"Values": [f"Measures[{value_measure}]"]},
          objects={"labels": props(fontSize=24, fontFamily="Segoe UI Semibold", color=C(NAVY)),
                   "categoryLabels": props(show=False)},
          vco=no_chrome())
    dyn_text(p, f"{key}_footer", x + 6, y + h - 34, w - 12, 30, footer_measure, size=9, color_measure=color_measure,
             bold=True)


def deneb(p: Page, vname, x, y, w, h, roles: list[str], spec: dict, title=None, subtitle=None):
    cfg = {"view": {"stroke": "transparent"}, "font": "Segoe UI", "background": "#FFFFFF",
           "legend": {"labelFontSize": 9, "titleFontSize": 9, "labelColor": GREY, "titleColor": GREY}}
    objs = {"vega": props(provider="vegaLite", jsonSpec=json.dumps(spec, separators=(",", ":")),
                          jsonConfig=json.dumps(cfg), isNewDialogOpen=False, enableTooltips=True,
                          enableContextMenu=True, enableHighlight=False, enableSelection=True,
                          selectionMaxDataPoints=50, version="6.4.1"),
            "developer": props(version="1.8.2.0")}
    p.add(vname, DENEB, x, y, w, h, roles={"dataset": roles}, objects=objs, title=title, subtitle=subtitle)


def nav(p: Page, pages: list[tuple[str, str, str]], active: str):
    shape(p, "nav_bg", 0, 0, 184, 720, "#FFFFFF")
    shape(p, "nav_edge", 184, 0, 1, 720, "#E6EAF0")
    shape(p, "nav_logo_mark", 18, 20, 34, 34, BLUE, radius=10)
    textbox(p, "nav_logo_icon", 18, 25, 34, 26, "CB", size=11, color="#FFFFFF", font="Segoe UI Black", center=True)
    textbox(p, "nav_logo", 60, 18, 120, 22, "Citi Bike", size=12, color=NAVY, font="Segoe UI Semibold")
    textbox(p, "nav_sub", 60, 38, 120, 18, "Live Ops BI · NYC", size=8, color=GREY)
    textbox(p, "nav_menu", 22, 80, 140, 18, "MENU", size=7, color="#8A93A6", font="Segoe UI Semibold")
    y = 100
    for pid, label, icon in pages:
        on = pid == active
        button(p, f"nav_{pid}", 10, y, 164, 38, f"{icon}    {label}", {"type": "PageNavigation", "navigationSection": pid},
               color=BLUE if on else GREY, hover_color=BLUE if on else NAVY, fill="#E8F0FD" if on else None,
               hover_fill="#E8F0FD" if on else "#F3F5F8", bold=on, radius=19, size=11)
        y += 44
    shape(p, "nav_card", 12, 600, 160, 104, "#F3F5F8", radius=10)
    textbox(p, "nav_card_t", 22, 610, 140, 18, "DATA SOURCES", size=7, color="#8A93A6", font="Segoe UI Semibold")
    textbox(p, "nav_card_b", 22, 628, 144, 70,
            "GBFS · trip files · NYC Open Data · Open-Meteo", size=8, color=GREY)


def header(p: Page, title: str):
    textbox(p, "hdr_title", 204, 10, 760, 34, title, size=20, color=NAVY, font="Segoe UI Semibold")
    dyn_text(p, "hdr_asof", 204, 44, 900, 22, "Data As Of Text", size=10, color=GREY)


def filter_pane(p: Page) -> tuple[list[str], dict]:
    """Hidden slide-in filter panel + open/close buttons. Returns (panel visual names, bookmarks)."""
    open_bm, close_bm = f"bm_{p.name}_filters_open", f"bm_{p.name}_filters_closed"
    button(p, "btn_filters", 1156, 16, 108, 32, "⚲   Filters", {"type": "Bookmark", "bookmark": open_bm},
           color=BLUE, hover_color=NAVY, fill="#FFFFFF", hover_fill="#E8F0FD", outline="#D6DEEB", bold=True,
           align="center", margin=0, radius=16)
    gx, gy, gw, gh = 1004, 58, 268, 650
    names = ["fp_group", "fp_bg", "fp_title", "fp_close", "fp_borough", "fp_date", "fp_member", "fp_ride", "fp_reset"]
    p.group("fp_group", "Filter pane", gx, gy, gw, gh, hidden=True)
    shape(p, "fp_bg", gx, gy, gw, gh, "#FFFFFF", radius=10, parent="fp_group", hidden=True, outline="#E6EAF0", shadow=True)
    textbox(p, "fp_title", gx + 16, gy + 12, 160, 26, "Filters", size=13, color=NAVY, font="Segoe UI Semibold",
            parent="fp_group", hidden=True)
    button(p, "fp_close", gx + gw - 44, gy + 10, 32, 28, "✕", {"type": "Bookmark", "bookmark": close_bm},
           color=GREY, align="center", margin=0, radius=14, parent="fp_group", hidden=True)
    sl = [("fp_borough", "dim_neighborhood[borough]", "Borough", "Dropdown", 56),
          ("fp_date", "dim_date[date]", "Dates", "Between", 92),
          ("fp_member", "fact_trips_hourly[member_casual]", "Rider type", "Basic", 92),
          ("fp_ride", "fact_trips_hourly[rideable_type]", "Bike type", "Basic", 92)]
    y = gy + 50
    for nm, field, ttl, mode, h in sl:
        p.add(nm, "slicer", gx + 12, y, gw - 24, h, roles={"Values": [field]}, parent="fp_group", hidden=True,
              objects={"data": props(mode=mode), "header": props(show=True, text=ttl, fontColor=C(GREY), textSize=9),
                       "items": props(fontColor=C(NAVY), textSize=9)},
              vco=no_chrome())
        y += h + 10
    button(p, "fp_reset", gx + 16, gy + gh - 48, gw - 32, 32, "Reset filters",
           {"type": "Bookmark", "bookmark": f"bm_{p.name}_reset"},
           color=BLUE, align="center", margin=0, outline="#D6DEEB", radius=16, parent="fp_group", hidden=True)
    return names, {"open": open_bm, "close": close_bm, "reset": f"bm_{p.name}_reset"}


# =============================================================================== map specs
def topo_inline() -> dict:
    import topojson as tp
    import geopandas as gpd

    g = gpd.read_file(PBI / "maps" / "nyc_nta.geojson")
    topo = tp.Topology(g[["nta_code", "nta_name", "borough", "in_service_area", "geometry"]], prequantize=1e4,
                       toposimplify=0.0006, object_name="nta")
    return json.loads(topo.to_json())


def base_layer(topo):
    return {"data": {"values": topo, "format": {"type": "topojson", "feature": "nta"}},
            "mark": {"type": "geoshape", "fill": "#EEF1F5", "stroke": "#FFFFFF", "strokeWidth": 0.6}}


STATUS_SCALE = {"domain": ["Empty", "Low", "Balanced", "High", "Full"],
                "range": ["#B2182B", "#F4A582", "#92C5DE", "#4393C3", "#2166AC"]}


def spec_overview(topo):
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": "container", "height": "container", "autosize": {"type": "fit", "contains": "padding"},
        "projection": {"type": "mercator"},
        "resolve": {"scale": {"color": "independent", "size": "independent"}, "legend": {"color": "independent"}},
        "layer": [
            base_layer(topo),
            {"data": {"name": "dataset"},
             "transform": [
                 {"filter": "isValid(datum['NTA Color Index'])"},
                 {"aggregate": [{"op": "max", "field": "NTA Color Index", "as": "index"},
                                {"op": "max", "field": "NTA Value Text", "as": "value"},
                                {"op": "count", "as": "stations"}],
                  "groupby": ["nta_code", "nta_name"]},
                 {"lookup": "nta_code", "from": {"data": {"values": topo, "format": {"type": "topojson", "feature": "nta"}},
                                                  "key": "properties.nta_code"}, "as": "geo"},
                 {"filter": "isValid(datum.geo)"}],
             "mark": {"type": "geoshape", "stroke": "#FFFFFF", "strokeWidth": 0.7, "cursor": "pointer"},
             "encoding": {
                 "shape": {"field": "geo", "type": "geojson"},
                 "color": {"field": "index", "type": "quantitative",
                           "scale": {"scheme": "redblue", "domain": [40, 160], "domainMid": 100, "clamp": True},
                           "legend": {"title": "Index vs city avg (100)", "orient": "bottom-left", "direction": "horizontal",
                                      "gradientLength": 150, "values": [40, 70, 100, 130, 160]}},
                 "tooltip": [{"field": "nta_name", "title": "Neighborhood"}, {"field": "value", "title": "Value"},
                             {"field": "index", "title": "Index (city = 100)", "format": ".0f"},
                             {"field": "stations", "title": "Stations"}]}},
            {"data": {"name": "dataset"},
             "transform": [{"filter": "isValid(datum['Station Status'])"}],
             "mark": {"type": "circle", "stroke": "#FFFFFF", "strokeWidth": 0.5, "opacity": 0.95},
             "encoding": {
                 "longitude": {"field": "lon", "type": "quantitative"},
                 "latitude": {"field": "lat", "type": "quantitative"},
                 "size": {"field": "Station Capacity", "type": "quantitative", "scale": {"range": [3, 36]}, "legend": None},
                 "color": {"field": "Station Status", "type": "nominal", "scale": STATUS_SCALE,
                           "legend": {"title": None, "orient": "top-right", "direction": "horizontal", "symbolSize": 60}},
                 "opacity": {"condition": {"test": {"field": "__selected__", "equal": "off"}, "value": 0.25}, "value": 0.95},
                 "tooltip": [{"field": "name", "title": "Station"}, {"field": "Station Status", "title": "Status"},
                             {"field": "Station Bikes", "title": "Bikes"}, {"field": "Station Docks", "title": "Docks"},
                             {"field": "NTA Value Text", "title": "Neighborhood"}]}},
        ],
    }


def spec_stations(topo, color_field, scale, size_field, tooltip, legend_title=None):
    color = {"field": color_field, "type": "nominal" if scale is STATUS_SCALE else "quantitative", "scale": scale,
             "legend": {"title": legend_title, "orient": "top-right", "direction": "horizontal"}}
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": "container", "height": "container", "autosize": {"type": "fit", "contains": "padding"},
        "projection": {"type": "mercator"},
        "resolve": {"scale": {"color": "independent"}},
        "layer": [
            base_layer(topo),
            {"data": {"name": "dataset"},
             "transform": [{"filter": f"isValid(datum['{color_field}'])"},
                           {"calculate": f"abs(datum['{size_field}'])", "as": "_size"}],
             "mark": {"type": "circle", "stroke": "#FFFFFF", "strokeWidth": 0.4},
             "encoding": {
                 "longitude": {"field": "lon", "type": "quantitative"},
                 "latitude": {"field": "lat", "type": "quantitative"},
                 "size": {"field": "_size", "type": "quantitative", "scale": {"range": [3, 40]}, "legend": None},
                 "color": color,
                 "opacity": {"condition": {"test": {"field": "__selected__", "equal": "off"}, "value": 0.2}, "value": 0.9},
                 "tooltip": tooltip}},
        ],
    }


def spec_routes(topo):
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": "container", "height": "container", "autosize": {"type": "fit", "contains": "padding"},
        "projection": {"type": "mercator"},
        "layer": [
            base_layer(topo),
            {"data": {"name": "dataset"},
             "transform": [{"window": [{"op": "rank", "as": "r"}], "sort": [{"field": "Route Trips", "order": "descending"}]},
                           {"filter": "datum.r <= 60"}],
             "mark": {"type": "rule", "strokeCap": "round", "color": "#2166AC", "opacity": 0.55},
             "encoding": {
                 "longitude": {"field": "start_lon", "type": "quantitative"},
                 "latitude": {"field": "start_lat", "type": "quantitative"},
                 "longitude2": {"field": "end_lon"}, "latitude2": {"field": "end_lat"},
                 "strokeWidth": {"field": "Route Trips", "type": "quantitative", "scale": {"range": [0.6, 6]}, "legend": None},
                 "tooltip": [{"field": "route", "title": "Route"}, {"field": "Route Trips", "title": "Trips", "format": ",.0f"},
                             {"field": "Route Member Share", "title": "Members", "format": ".0%"}]}},
            {"data": {"name": "dataset"},
             "transform": [{"window": [{"op": "rank", "as": "r"}], "sort": [{"field": "Route Trips", "order": "descending"}]},
                           {"filter": "datum.r <= 60"}],
             "mark": {"type": "circle", "color": "#1F2A44", "size": 14},
             "encoding": {"longitude": {"field": "start_lon", "type": "quantitative"},
                          "latitude": {"field": "start_lat", "type": "quantitative"}}},
        ],
    }


# =============================================================================== pages
PAGES = [("overview", "Overview", "◎"), ("liveops", "Live Operations", "⚡"), ("demand", "Demand Patterns", "▦"),
         ("customers", "Customers", "◔"), ("network", "Network & Routes", "⇄"), ("dataquality", "Data Quality", "✓")]
TOOLTIP, DETAIL = "nta_tooltip", "station_detail"
X0, W = 204, 1060
COLS4 = [(X0 + i * (256 + 12), 256) for i in range(4)]


def page_common(pid, title) -> tuple[Page, list[str], dict]:
    p = Page(pid, dict((a, b) for a, b, _ in PAGES)[pid])
    nav(p, PAGES, pid)
    header(p, title)
    fp, bms = filter_pane(p)
    return p, fp, bms


def kpi_row(p, items):
    for (x, w), (key, label, val, foot, colr) in zip(COLS4, items):
        kpi(p, key, x, 72, w, label, val, foot, colr)


def build_pages(topo) -> tuple[list[Page], list[dict]]:
    pages, bookmarks = [], []

    # ---------------------------------------------------------------- 1 Overview
    p, fp, bm = page_common("overview", "Overview")
    kpi_row(p, [("k_trips", "Trips", "Total Trips", "KPI Footer Trips", "KPI Color Trips"),
                ("k_dur", "Avg ride (min)", "Avg Duration (min)", "KPI Footer Duration", "KPI Color Neutral"),
                ("k_mem", "Member share", "Member Share %", "KPI Footer Member", "KPI Color Member"),
                ("k_bal", "Balanced now", "% Stations Balanced", "KPI Footer Balanced", "KPI Color Balanced")])
    p.add("ov_metric", "slicer", X0, 188, 640, 40, roles={"Values": ["Map Metric[Metric]"]},
          objects={"data": props(mode="Basic"), "general": props(orientation=1, responsive=True),
                   "selection": props(singleSelect=True, strictSingleSelect=True, selectAllCheckboxEnabled=False),
                   "header": props(show=False), "items": props(fontColor=C(NAVY), textSize=9, background=C("#FFFFFF"),
                                                              outline="None")},
          vco={"title": props(show=False), "padding": props(top=4, bottom=4, left=8, right=8)})
    deneb(p, "ov_map", X0, 232, 640, 302,
          ["dim_station[short_name]", "dim_station[name]", "dim_station[lat]", "dim_station[lon]",
           "dim_station[nta_code]", "dim_station[nta_name]", "Measures[NTA Color Index]", "Measures[NTA Value Text]",
           "Measures[Station Status]", "Measures[Station Capacity]", "Measures[Station Bikes]", "Measures[Station Docks]"],
          spec_overview(topo), title="Neighborhood health (live)", subtitle="Action Map")
    p.add("ov_trend", "lineChart", X0 + 652, 188, 408, 168,
          roles={"Category": ["dim_date[date]"],
                 "Y": ["Measures[Classic Trips]", "Measures[E-bike Trips]", "Measures[Trips 7D Rolling Avg]"]},
          title="Daily trips", subtitle="Action Trend",
          objects={"lineStyles": [{"properties": {"strokeWidth": L(2)}},
                                  {"properties": {"strokeWidth": L(2), "lineStyle": L("dashed")},
                                   "selector": {"metadata": "_Measures.Trips 7D Rolling Avg"}}],
                   "dataPoint": [{"properties": {"fill": C("#92C5DE")}, "selector": {"metadata": "_Measures.Classic Trips"}},
                                 {"properties": {"fill": C("#2F6FDE")}, "selector": {"metadata": "_Measures.E-bike Trips"}},
                                 {"properties": {"fill": C("#B2182B")}, "selector": {"metadata": "_Measures.Trips 7D Rolling Avg"}}]})
    p.add("ov_borough", "clusteredBarChart", X0 + 652, 366, 408, 168,
          roles={"Category": ["dim_neighborhood[borough]"], "Y": ["Measures[Trips per 1k Residents]"]},
          sort=("Measures[Trips per 1k Residents]", "Descending"),
          title="Trips per 1k residents", subtitle="Action Borough",
          tooltip_page=TOOLTIP,
          objects={"dataPoint": [{"properties": {"fill": CM("Bar Color vs City (trips per 1k)")},
                                  "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}]}}],
                   "labels": props(show=True, fontSize=8),
                   "xAxisReferenceLine": [{"properties": {"show": L(True), "value": {"expr": MEAS("City Trips per 1k Residents")},
                                                          "lineColor": C(NAVY), "style": L("dashed"), "displayName": L("City avg"),
                                                          "dataLabelShow": L(True)}, "selector": {"id": "1"}}]})
    p.add("ov_donut", "donutChart", X0, 546, 300, 162,
          roles={"Category": ["fact_trips_hourly[member_casual]"], "Y": ["Measures[Total Trips]"]},
          title="Members vs casual", subtitle="Action Members",
          objects={"legend": props(show=True, position="Right"), "labels": props(show=True, labelStyle="Percent of total"),
                   "dataPoint": [{"properties": {"fill": C("#2166AC")}, "selector": {"data": [{"scopeId": {"Comparison": {
                       "ComparisonKind": 0, "Left": COL("fact_trips_hourly", "member_casual"), "Right": {"Literal": {"Value": "'member'"}}}}}]}},
                       {"properties": {"fill": C("#F4A582")}, "selector": {"data": [{"scopeId": {"Comparison": {
                           "ComparisonKind": 0, "Left": COL("fact_trips_hourly", "member_casual"), "Right": {"Literal": {"Value": "'casual'"}}}}}]}}]})
    p.add("ov_nbhd", "tableEx", X0 + 312, 546, 748, 162,
          roles={"Values": ["dim_neighborhood[nta_name]|Neighborhood", "dim_neighborhood[borough]|Borough",
                            "Measures[Trips per 1k Residents]|Trips / 1k res.", "Measures[Trips per 1k Index]|Index (city=100)",
                            "Measures[Trips WoW %]|WoW", "Measures[% Stations Balanced]|Balanced now",
                            "Measures[Utilization Index]|Utilization idx", "Measures[Recommended Action]|→ Action"]},
          sort=("Measures[Trips per 1k Index]", "Descending"),
          title="Neighborhoods (city = 100)", subtitle="Action Neighborhoods",
          tooltip_page=TOOLTIP,
          objects={"columnFormatting": [
              {"properties": {"dataBars": {"positiveColor": C("#92C5DE"), "negativeColor": C("#F4A582"), "axisColor": C(GREY),
                                           "reverseDirection": L(False), "hideText": L(False)}},
               "selector": {"metadata": "_Measures.Trips per 1k Index"}}],
              "values": [{"properties": {"backColor": CM("Action Color")},
                          "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}], "metadata": "_Measures.Recommended Action"}}]})
    pages.append(p); bookmarks += make_bookmarks(p, fp, bm)

    # ---------------------------------------------------------------- 2 Live operations
    p, fp, bm = page_common("liveops", "Live Operations")
    kpi_row(p, [("k_empty", "Empty now", "Empty Stations (now)", "KPI Footer Empty", "KPI Color Empty"),
                ("k_full", "Full now", "Full Stations (now)", "KPI Footer Full", "KPI Color Full"),
                ("k_bikes", "Bikes", "Bikes Available", "KPI Footer Bikes", "KPI Color Neutral"),
                ("k_docks", "Docks", "Docks Available", "KPI Footer Docks", "KPI Color Neutral")])
    deneb(p, "lo_map", X0, 188, 470, 520,
          ["dim_station[short_name]", "dim_station[name]", "dim_station[lat]", "dim_station[lon]",
           "Measures[Station Status]", "Measures[Station Capacity]", "Measures[Station Bikes]", "Measures[Station Docks]",
           "Measures[Station Min to Empty]", "Measures[Station Priority]"],
          spec_stations(topo, "Station Status", STATUS_SCALE, "Station Capacity",
                        [{"field": "name", "title": "Station"}, {"field": "Station Status", "title": "Status"},
                         {"field": "Station Bikes", "title": "Bikes"}, {"field": "Station Docks", "title": "Docks"},
                         {"field": "Station Min to Empty", "title": "Min to empty", "format": ".0f"},
                         {"field": "Station Priority", "title": "Priority", "format": ".0f"}]),
          title="Stations now", subtitle="Action Live Map")
    p.add("lo_dispatch", "tableEx", X0 + 482, 188, 578, 520,
          roles={"Values": ["fact_rebalance_list[priority_rank]|#", "dim_station[name]|Station", "fact_rebalance_list[borough]|Borough",
                            "fact_rebalance_list[status_class]|Status", "fact_rebalance_list[num_bikes_available]|Bikes",
                            "fact_rebalance_list[num_docks_available]|Docks", "fact_rebalance_list[est_minutes_to_empty]|Min to empty",
                            "fact_rebalance_list[est_minutes_to_full]|Min to full", "fact_rebalance_list[rebalance_priority_score]|Priority",
                            "fact_rebalance_list[action]|→ Action"]},
          sort=("fact_rebalance_list[priority_rank]", "Ascending"),
          title="Dispatch list", subtitle="Action Dispatch",
          objects={"columnFormatting": [
              {"properties": {"dataBars": {"positiveColor": C("#F4A582"), "negativeColor": C("#92C5DE"), "axisColor": C(GREY),
                                           "reverseDirection": L(False), "hideText": L(False)}},
               "selector": {"metadata": "fact_rebalance_list.rebalance_priority_score"}}]})
    pages.append(p); bookmarks += make_bookmarks(p, fp, bm)

    # ---------------------------------------------------------------- 3 Demand patterns
    p, fp, bm = page_common("demand", "Demand Patterns")
    kpi_row(p, [("k_peak", "Peak hour", "Peak Hour Label", "KPI Footer Peak", "KPI Color Neutral"),
                ("k_wd", "Busiest weekday", "Peak Weekday Label", "KPI Footer Weekday", "KPI Color Neutral"),
                ("k_we", "Weekend / weekday", "Weekend vs Weekday", "KPI Footer Weekend", "KPI Color Neutral"),
                ("k_rain", "Rain impact", "Rain Impact %", "KPI Footer Rain", "KPI Color Neutral")])
    p.add("dm_heat", "pivotTable", X0, 188, 640, 250,
          roles={"Rows": ["dim_date[weekday_name]"], "Columns": ["dim_hour[hour]"], "Values": ["Measures[Avg Trips per Hour]"]},
          title="Trips per hour", subtitle="Action Heatmap",
          objects={"values": [{"properties": {"backColor": CM("Heat Color"), "fontColor": CM("Heat Font Color")},
                               "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}],
                                            "metadata": "_Measures.Avg Trips per Hour"}}],
                   "grid": props(gridVertical=False, gridHorizontal=False, rowPadding=3, textSize=8),
                   "columnHeaders": props(fontSize=8), "rowHeaders": props(fontSize=9)})
    p.add("dm_profile", "lineChart", X0 + 652, 188, 408, 250,
          roles={"Category": ["dim_hour[hour]"],
                 "Y": ["Measures[Weekday Avg Trips per Hour]", "Measures[Weekend Avg Trips per Hour]"]},
          title="Weekday vs weekend", subtitle="Action Hourly",
          objects={"dataPoint": [{"properties": {"fill": C("#2166AC")}, "selector": {"metadata": "_Measures.Weekday Avg Trips per Hour"}},
                                 {"properties": {"fill": C("#F4A582")}, "selector": {"metadata": "_Measures.Weekend Avg Trips per Hour"}}],
                   "lineStyles": props(strokeWidth=2)})
    deneb(p, "dm_flow", X0, 450, 640, 258,
          ["dim_station[short_name]", "dim_station[name]", "dim_station[lat]", "dim_station[lon]",
           "Measures[AM Net Flow per Weekday]", "Measures[PM Net Flow per Weekday]"],
          spec_stations(topo, "AM Net Flow per Weekday",
                        {"scheme": "redblue", "domain": [-30, 30], "domainMid": 0, "clamp": True},
                        "AM Net Flow per Weekday",
                        [{"field": "name", "title": "Station"},
                         {"field": "AM Net Flow per Weekday", "title": "AM net flow / weekday", "format": "+.1f"},
                         {"field": "PM Net Flow per Weekday", "title": "PM net flow / weekday", "format": "+.1f"}],
                        legend_title="AM net flow (red = drains)"),
          title="Morning drain (red) / fill (blue)", subtitle="Action Net Flow")
    p.add("dm_weather", "clusteredColumnChart", X0 + 652, 450, 408, 258,
          roles={"Category": ["fact_weather_hourly[weather_class]"], "Y": ["Measures[Trips vs Normal]"]},
          title="Weather impact", subtitle="Action Weather",
          objects={"dataPoint": [{"properties": {"fill": C("#4393C3")}}], "labels": props(show=True, fontSize=8),
                   "y1AxisReferenceLine": [{"properties": {"show": L(True), "value": L(1), "lineColor": C(NAVY),
                                                           "style": L("dashed"), "displayName": L("Normal")},
                                            "selector": {"id": "1"}}]})
    pages.append(p); bookmarks += make_bookmarks(p, fp, bm)

    # ---------------------------------------------------------------- 4 Customers
    p, fp, bm = page_common("customers", "Customers")
    kpi_row(p, [("k_mem", "Member share", "Member Share %", "KPI Footer Member", "KPI Color Member"),
                ("k_cas", "Casual share", "Casual Share %", "KPI Footer Casual", "KPI Color Neutral"),
                ("k_ebk", "E-bike share", "E-bike Share %", "KPI Footer Ebike", "KPI Color Neutral"),
                ("k_rev", "Revenue upside", "Revenue Opportunity", "KPI Footer Revenue", "KPI Color Neutral")])
    p.add("cu_dur", "clusteredBarChart", X0, 188, 340, 250,
          roles={"Category": ["fact_trips_hourly[rideable_type]"], "Series": ["fact_trips_hourly[member_casual]"],
                 "Y": ["Measures[Avg Duration (min)]"]},
          title="Ride length (min)", subtitle="Action Duration",
          objects={"labels": props(show=True, fontSize=8)})
    p.add("cu_hour", "lineChart", X0 + 352, 188, 708, 250,
          roles={"Category": ["dim_hour[hour]"], "Y": ["Measures[Casual Share %]"]},
          title="Casual share by hour", subtitle="Action Time of Day",
          objects={"dataPoint": props(fill=C("#D6604D")), "lineStyles": props(strokeWidth=2),
                   "y1AxisReferenceLine": [{"properties": {"show": L(True), "value": {"expr": MEAS("Casual Share %")},
                                                           "lineColor": C(GREY), "style": L("dashed"), "displayName": L("Day avg")},
                                            "selector": {"id": "1"}}]})
    p.add("cu_week", "hundredPercentStackedColumnChart", X0, 450, 340, 258,
          roles={"Category": ["dim_date[weekday_name]"], "Series": ["fact_trips_hourly[rideable_type]"],
                 "Y": ["Measures[Total Trips]"]},
          title="Bike mix by day", subtitle="Action Ebike")
    p.add("cu_whatif_p", "slicer", X0 + 352, 450, 200, 60, roles={"Values": ["Membership Price[Price]"]},
          objects={"data": props(mode="Dropdown"), "header": props(show=True, text="Membership price", textSize=9)},
          vco={"title": props(show=False)})
    p.add("cu_whatif_r", "slicer", X0 + 352, 518, 200, 60, roles={"Values": ["Conversion Rate[Rate]"]},
          objects={"data": props(mode="Dropdown"), "header": props(show=True, text="Conversion rate", textSize=9)},
          vco={"title": props(show=False)})
    kpi(p, "k_targets", X0 + 352, 586, 200, "Target riders", "Conversion Targets (est. riders)",
        "KPI Footer Revenue")
    p.add("cu_targets", "tableEx", X0 + 564, 450, 496, 258,
          roles={"Values": ["dim_station[name]|Start station", "dim_station[nta_name]|Neighborhood",
                            "Measures[Casual Repeat Commute Trips]|Repeat casual commute trips", "Measures[Casual Share %]|Casual share"]},
          sort=("Measures[Casual Repeat Commute Trips]", "Descending"),
          title="Campaign targets", subtitle="Action Conversion",
          objects={"columnFormatting": [
              {"properties": {"dataBars": {"positiveColor": C("#92C5DE"), "negativeColor": C("#F4A582"), "axisColor": C(GREY),
                                           "reverseDirection": L(False), "hideText": L(False)}},
               "selector": {"metadata": "_Measures.Casual Repeat Commute Trips"}}]})
    pages.append(p); bookmarks += make_bookmarks(p, fp, bm)

    # ---------------------------------------------------------------- 5 Network & routes
    p, fp, bm = page_common("network", "Network & Routes")
    kpi_row(p, [("k_route", "Top route / day", "Top Route Trips per Day", "KPI Footer Routes", "KPI Color Neutral"),
                ("k_dist", "Avg distance (km)", "Avg Distance (km)", "KPI Footer Distance", "KPI Color Neutral"),
                ("k_util", "Trips / dock / day", "City Trips per Dock per Day", "KPI Footer Utilization", "KPI Color Neutral"),
                ("k_under", "Under-served areas", "Under-served NTAs", "KPI Footer Underserved", "KPI Color Neutral")])
    deneb(p, "nw_flows", X0, 188, 420, 290,
          ["fact_top_routes[route]", "fact_top_routes[start_lat]", "fact_top_routes[start_lon]",
           "fact_top_routes[end_lat]", "fact_top_routes[end_lon]", "Measures[Route Trips]", "Measures[Route Member Share]"],
          spec_routes(topo), title="Top flows", subtitle="Action Routes")
    p.add("nw_routes", "tableEx", X0 + 432, 188, 628, 290,
          roles={"Values": ["fact_top_routes[route_rank]|#", "fact_top_routes[route]|Route", "fact_top_routes[start_borough]|Borough",
                            "Measures[Route Trips per Day]|Trips / day", "Measures[Route Member Share]|Members",
                            "fact_top_routes[avg_distance_km]|Km", "fact_top_routes[avg_duration_min]|Min"]},
          sort=("fact_top_routes[route_rank]", "Ascending"),
          title="Top routes", subtitle="Action Routes",
          objects={"columnFormatting": [
              {"properties": {"dataBars": {"positiveColor": C("#92C5DE"), "negativeColor": C("#F4A582"), "axisColor": C(GREY),
                                           "reverseDirection": L(False), "hideText": L(False)}},
               "selector": {"metadata": "_Measures.Route Trips per Day"}}]})
    p.add("nw_util", "clusteredBarChart", X0, 490, 420, 218,
          roles={"Category": ["dim_station[name]"], "Y": ["Measures[Trips per Dock per Day]"]},
          sort=("Measures[Trips per Dock per Day]", "Descending"),
          title="Busiest docks", subtitle="Action Utilization",
          objects={"dataPoint": [{"properties": {"fill": CM("Bar Color vs City (utilization)")},
                                  "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}]}}],
                   "xAxisReferenceLine": [{"properties": {"show": L(True), "value": {"expr": MEAS("City Trips per Dock per Day")},
                                                          "lineColor": C(NAVY), "style": L("dashed"), "displayName": L("City avg")},
                                           "selector": {"id": "1"}}]})
    p.visuals[-1]["filterConfig"] = {"filters": [{
        "name": "topn_util", "type": "TopN", "howCreated": "User",
        "field": COL("dim_station", "name"),
        "filter": {"Version": 2,
                   "From": [{"Name": "subquery", "Expression": {"Subquery": {"Query": {
                       "Version": 2, "From": [{"Name": "d", "Entity": "dim_station", "Type": 0},
                                              {"Name": "m", "Entity": "_Measures", "Type": 0}],
                       "Select": [{"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "name"}, "Name": "field"}],
                       "OrderBy": [{"Direction": 2, "Expression": {"Measure": {"Expression": {"SourceRef": {"Source": "m"}},
                                                                               "Property": "Trips per Dock per Day"}}}],
                       "Top": 15}}}, "Type": 2},
                            {"Name": "d", "Entity": "dim_station", "Type": 0}],
                   "Where": [{"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "name"}}],
                                                   "Table": {"SourceRef": {"Source": "subquery"}}}}}]}}]}
    p.add("nw_expand", "tableEx", X0 + 432, 490, 628, 218,
          roles={"Values": ["dim_neighborhood[nta_name]|Neighborhood", "dim_neighborhood[borough]|Borough", "Measures[Population]|Residents",
                            "Measures[Docks per 1k Residents]|Docks / 1k", "Measures[Trips per 1k Index]|Trips idx",
                            "Measures[Utilization Index]|Utilization idx", "Measures[Recommended Action]|→ Action"]},
          sort=("Measures[Docks per 1k Residents]", "Ascending"),
          title="Expansion candidates", subtitle="Action Expansion",
          tooltip_page=TOOLTIP,
          objects={"values": [{"properties": {"backColor": CM("Action Color")},
                               "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}],
                                            "metadata": "_Measures.Recommended Action"}}]})
    p.visuals[-1]["filterConfig"] = {"filters": [{
        "name": "svc_area", "type": "Categorical", "howCreated": "User", "field": COL("dim_neighborhood", "in_service_area"),
        "filter": {"Version": 2, "From": [{"Name": "n", "Entity": "dim_neighborhood", "Type": 0}],
                   "Where": [{"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "n"}},
                                                                               "Property": "in_service_area"}}],
                                                   "Values": [[{"Literal": {"Value": "true"}}]]}}}]}}]}
    pages.append(p); bookmarks += make_bookmarks(p, fp, bm)

    # ---------------------------------------------------------------- 6 Data quality
    p, fp, bm = page_common("dataquality", "Data Quality & Pipeline")
    kpi_row(p, [("k_fresh", "Data age (min)", "Data Freshness (min)", "KPI Footer Freshness", "KPI Color Freshness"),
                ("k_rem", "Rows removed", "Rows Removed by Cleaning %", "KPI Footer Removed", "KPI Color Neutral"),
                ("k_unm", "Unmatched IDs", "Unmatched Station IDs", "KPI Footer Unmatched", "KPI Color Neutral"),
                ("k_runs", "Run success", "Pipeline Success Rate", "KPI Footer Runs", "KPI Color Neutral")])
    p.add("dq_rules", "clusteredBarChart", X0, 188, 524, 280,
          roles={"Category": ["fact_trip_cleaning[rule]|Rule"], "Series": ["fact_trip_cleaning[action]|Action"],
                 "Y": ["Measures[Rows Affected]|Rows affected"]},
          sort=("Measures[Rows Affected]", "Descending"),
          title="Cleaning rules", subtitle="Action Data Quality",
          objects={"labels": props(show=True, fontSize=8)})
    p.add("dq_live", "clusteredBarChart", X0 + 536, 188, 524, 280,
          roles={"Category": ["fact_data_quality[rule]|Rule"], "Y": ["Measures[Live Rows Affected]|Rows affected"]},
          sort=("Measures[Live Rows Affected]", "Descending"),
          title="Live data checks", subtitle="Action Pipeline",
          objects={"dataPoint": [{"properties": {"fill": C("#F4A582")}}], "labels": props(show=True, fontSize=8)})
    p.add("dq_runs", "tableEx", X0, 480, 524, 228,
          roles={"Values": ["fact_pipeline_runs[run_time_utc_ny]|Run time (ET)", "fact_pipeline_runs[mode]|Mode", "fact_pipeline_runs[status]|Status",
                            "fact_pipeline_runs[duration_s]|Seconds", "fact_pipeline_runs[error]|Error"]},
          sort=("fact_pipeline_runs[run_time_utc_ny]", "Descending"), title="Run log")
    p.add("dq_unmatched", "tableEx", X0 + 536, 480, 524, 228,
          roles={"Values": ["fact_unmatched_stations[station_id]|Station ID", "fact_unmatched_stations[station_name]|Name",
                            "fact_unmatched_stations[trips]|Trips"]},
          sort=("fact_unmatched_stations[trips]", "Descending"),
          title="Unmatched stations")
    pages.append(p); bookmarks += make_bookmarks(p, fp, bm)

    # ---------------------------------------------------------------- Station detail (drill-through)
    p = Page(DETAIL, "Station Detail", hidden=True)
    p.extra = {
        "pageBinding": {"name": "station_detail_binding", "type": "Drillthrough", "referenceScope": "Default",
                        "parameters": [{"name": "p_station", "boundFilter": "f_station", "fieldExpr": COL("dim_station", "name")}]},
        "filterConfig": {"filters": [{"name": "f_station", "type": "Categorical", "howCreated": "Drillthrough",
                                      "field": COL("dim_station", "name")}]},
    }
    nav(p, PAGES, "liveops")
    textbox(p, "sd_title", X0, 10, 600, 34, "Station Detail", size=20, color=NAVY, font="Segoe UI Semibold")
    dyn_text(p, "sd_action", X0, 44, 700, 22, "Action Station", size=11, color=BLUE, bold=True)
    button(p, "sd_back", X0 + 960, 16, 100, 32, "←   Back", {"type": "Back"}, color=BLUE, hover_color=NAVY,
           fill="#FFFFFF", hover_fill="#E8F0FD", outline="#D6DEEB", bold=True, align="center", margin=0, radius=16)
    p.add("sd_pick", "slicer", X0 + 720, 12, 228, 50, roles={"Values": ["dim_station[name]|Station"]},
          objects={"data": props(mode="Dropdown"), "header": props(show=True, text="Pick a station", fontColor=C(GREY), textSize=9),
                   "items": props(fontColor=C(NAVY), textSize=10)},
          vco=no_chrome())
    kpi_row(p, [("s_status", "Status now", "Station Status", "Station Footer Status", "KPI Color Neutral"),
                ("s_bikes", "Bikes now", "Station Bikes", "Station Footer Bikes", "KPI Color Neutral"),
                ("s_cap", "Docks", "Station Capacity", "Station Footer Capacity", "KPI Color Neutral"),
                ("s_trips", "Trips / day", "Trips per Day", "KPI Footer Trips", "KPI Color Trips")])
    p.add("sd_profile", "lineChart", X0, 188, 524, 250,
          roles={"Category": ["dim_hour[hour]|Hour"],
                 "Y": ["Measures[Departures]|Departures", "Measures[Arrivals]|Arrivals"]},
          title="Departures vs arrivals", subtitle="Action Station Flow",
          objects={"dataPoint": [{"properties": {"fill": C("#D6604D")}, "selector": {"metadata": "_Measures.Departures"}},
                                 {"properties": {"fill": C("#2166AC")}, "selector": {"metadata": "_Measures.Arrivals"}}],
                   "lineStyles": props(strokeWidth=3)})
    p.add("sd_history", "areaChart", X0 + 536, 188, 524, 250,
          roles={"Category": ["fact_station_status[snapshot_utc_ny]|Snapshot (ET)"], "Y": ["Measures[Snapshot Bikes]|Bikes"]},
          title="Bikes over time", subtitle="Action Snapshot",
          objects={"dataPoint": props(fill=C(BLUE)), "lineStyles": props(strokeWidth=2)})
    p.add("sd_heat", "pivotTable", X0, 450, 524, 258,
          roles={"Rows": ["dim_date[weekday_name]|Day"], "Columns": ["dim_hour[hour]|Hour"],
                 "Values": ["Measures[Avg Trips per Hour]|Trips"]},
          title="Busy hours",
          objects={"values": [{"properties": {"backColor": CM("Heat Color"), "fontColor": CM("Heat Font Color")},
                               "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}],
                                            "metadata": "_Measures.Avg Trips per Hour"}}],
                   "grid": props(gridVertical=False, gridHorizontal=False, rowPadding=3, textSize=8),
                   "columnHeaders": props(fontSize=8), "rowHeaders": props(fontSize=9)})
    p.add("sd_dest", "tableEx", X0 + 536, 450, 524, 258,
          roles={"Values": ["fact_top_routes[route]|Route", "Measures[Route Trips per Day]|Trips / day",
                            "Measures[Route Member Share]|Members", "fact_top_routes[avg_distance_km]|Km"]},
          sort=("Measures[Route Trips per Day]", "Descending"), title="Top routes from here",
          objects={"columnFormatting": [
              {"properties": {"dataBars": {"positiveColor": C("#92C5DE"), "negativeColor": C("#F4A582"), "axisColor": C(GREY),
                                           "reverseDirection": L(False), "hideText": L(False)}},
               "selector": {"metadata": "_Measures.Route Trips per Day"}}]})
    pages.append(p)

    # ---------------------------------------------------------------- Tooltip page
    p = Page(TOOLTIP, "Neighborhood Tooltip", width=340, height=280, ptype="Tooltip", hidden=True)
    p.extra = {"pageBinding": {"name": "nta_tooltip_binding", "type": "Tooltip", "referenceScope": "Default"}}
    shape(p, "tt_bg", 0, 0, 340, 280, "#FFFFFF")
    shape(p, "tt_accent", 14, 0, 40, 4, BLUE, radius=2)
    dyn_text(p, "tt_name", 12, 10, 316, 26, "Tooltip Title", size=13, color=NAVY, bold=True)
    dyn_text(p, "tt_rank", 12, 36, 316, 20, "Tooltip Rank", size=9, color=GREY)
    tiles = [("Bikes", "Bikes Available"), ("Docks available", "Docks Available"),
             ("Empty stations", "Empty Stations (now)"), ("Full stations", "Full Stations (now)")]
    for i, (lab, meas) in enumerate(tiles):
        x, y = 12 + (i % 2) * 160, 60 + (i // 2) * 58
        shape(p, f"tt_tile{i}", x, y, 154, 52, "#F3F5F8", radius=8)
        textbox(p, f"tt_lab{i}", x + 8, y + 4, 140, 18, lab.upper(), size=7, color=GREY, font="Segoe UI Semibold")
        p.add(f"tt_val{i}", "card", x + 2, y + 18, 150, 32, roles={"Values": [f"Measures[{meas}]"]},
              objects={"labels": props(fontSize=15, fontFamily="Segoe UI Semibold", color=C(NAVY)),
                       "categoryLabels": props(show=False)}, vco=no_chrome())
    p.add("tt_spark", "areaChart", 8, 178, 324, 56,
          roles={"Category": ["fact_station_status[snapshot_utc_ny]"], "Y": ["Measures[Snapshot Bikes]"]},
          objects={"categoryAxis": props(show=False), "valueAxis": props(show=False), "legend": props(show=False),
                   "dataPoint": props(fill=C(BLUE))},
          vco=no_chrome())
    dyn_text(p, "tt_action", 12, 234, 316, 44, "Action Map", size=9, color=BLUE, bold=True)
    pages.append(p)
    return pages, bookmarks


def make_bookmarks(p: Page, panel: list[str], bm: dict) -> list[dict]:
    def bmk(name, display, hidden: bool, data=False):
        vcs = {v: {"singleVisual": {"visualType": "shape", "objects": {},
                                    **({"display": {"mode": "hidden"}} if hidden else {})}} for v in panel if v != "fp_group"}
        state = {"version": "1.3", "activeSection": p.name, "sections": {p.name: {"visualContainers": vcs}}}
        opts = {"targetVisualNames": panel, "applyOnlyToTargetVisuals": True, "suppressActiveSection": True,
                "suppressData": not data}
        return {"$schema": f"{SCHEMA}/bookmark/{V_BOOKMARK}/schema.json", "displayName": display, "name": name,
                "options": opts, "explorationState": state}

    reset = {"$schema": f"{SCHEMA}/bookmark/{V_BOOKMARK}/schema.json", "displayName": f"{p.display} · reset view",
             "name": bm["reset"], "options": {"suppressActiveSection": True, "suppressDisplay": True},
             "explorationState": {"version": "1.3", "activeSection": p.name, "sections": {p.name: {"visualContainers": {}}}}}
    return [bmk(bm["open"], f"{p.display} · filters open", False),
            bmk(bm["close"], f"{p.display} · filters closed", True), reset]


# =============================================================================== report writer
def build_report(pages: list[Page], bookmarks: list[dict]):
    clean_dir(RP)
    d = RP / "definition"
    (d / "pages").mkdir(parents=True, exist_ok=True)
    (RP / "definition.pbir").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0", "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}}}, indent=2), encoding="utf-8")
    (d / "version.json").write_text(json.dumps({
        "$schema": f"{SCHEMA}/versionMetadata/1.0.0/schema.json", "version": "2.0.0"}, indent=2), encoding="utf-8")

    # theme resources
    rr = RP / "StaticResources" / "RegisteredResources"
    sr = RP / "StaticResources" / "SharedResources" / "BaseThemes"
    rr.mkdir(parents=True)
    sr.mkdir(parents=True)
    shutil.copy(PBI / "theme" / "citibike-minimal.json", rr / "citibike-minimal.json")
    shutil.copy(Path(__file__).parent / "CY24SU10.json", sr / "CY24SU10.json")
    imp = {"visual": V_VISUAL, "report": V_REPORT, "page": V_PAGE}
    report = {
        "$schema": f"{SCHEMA}/report/{V_REPORT}/schema.json",
        "themeCollection": {"baseTheme": {"name": "CY24SU10", "reportVersionAtImport": imp, "type": "SharedResources"},
                            "customTheme": {"name": "citibike-minimal.json", "reportVersionAtImport": imp,
                                            "type": "RegisteredResources"}},
        "objects": {"outspacePane": props(visible=False, expanded=False),
                    "section": props(verticalAlignment="Middle")},
        "publicCustomVisuals": [DENEB],
        "resourcePackages": [
            {"name": "SharedResources", "type": "SharedResources",
             "items": [{"name": "CY24SU10", "path": "BaseThemes/CY24SU10.json", "type": "BaseTheme"}]},
            {"name": "RegisteredResources", "type": "RegisteredResources",
             "items": [{"name": "citibike-minimal.json", "path": "citibike-minimal.json", "type": "CustomTheme"}]}],
        "settings": {"useStylableVisualContainerHeader": True, "defaultDrillFilterOtherVisuals": True,
                     "allowChangeFilterTypes": True, "useEnhancedTooltips": True, "useDefaultAggregateDisplayName": True},
    }
    (d / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    (d / "pages" / "pages.json").write_text(json.dumps({
        "$schema": f"{SCHEMA}/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [p.name for p in pages], "activePageName": pages[0].name}, indent=2), encoding="utf-8")
    for p in pages:
        pd = d / "pages" / p.name
        (pd / "visuals").mkdir(parents=True)
        page = {"$schema": f"{SCHEMA}/page/{V_PAGE}/schema.json", "name": p.name, "displayName": p.display,
                "displayOption": "ActualSize" if p.ptype == "Tooltip" else "FitToPage", "height": p.h, "width": p.w}
        if p.ptype == "Tooltip":
            page["type"] = "Tooltip"
        if p.hidden:
            page["visibility"] = "HiddenInViewMode"
        page.update(p.extra)
        (pd / "page.json").write_text(json.dumps(page, indent=2, ensure_ascii=False), encoding="utf-8")
        for v in p.visuals:
            vd = pd / "visuals" / v["name"]
            vd.mkdir()
            (vd / "visual.json").write_text(json.dumps(v, indent=2, ensure_ascii=False), encoding="utf-8")

    bd = d / "bookmarks"
    bd.mkdir()
    for b in bookmarks:
        (bd / f"{b['name']}.bookmark.json").write_text(json.dumps(b, indent=2, ensure_ascii=False), encoding="utf-8")
    (bd / "bookmarks.json").write_text(json.dumps({
        "$schema": f"{SCHEMA}/bookmarksMetadata/1.0.0/schema.json",
        "items": [{"name": b["name"]} for b in bookmarks]}, indent=2), encoding="utf-8")

    (PBI / f"{NAME}.pbip").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
        "version": "1.0", "artifacts": [{"report": {"path": f"{NAME}.Report"}}],
        "settings": {"enableAutoRecovery": True}}, indent=2), encoding="utf-8")


def main():
    base = repo_base_url()
    tables = build_model(base)
    pages, bms = build_pages(topo_inline())
    build_report(pages, bms)
    nvis = sum(len(p.visuals) for p in pages)
    print(f"BaseUrl: {base}")
    print(f"Model: {len(tables)} tables, {len(MD.M)} measures, {len(MD.RELATIONSHIPS)} relationships")
    print(f"Report: {len(pages)} pages, {nvis} visuals, {len(bms)} bookmarks")


if __name__ == "__main__":
    main()
