# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import pandas as pd
import requests

import geopandas as gpd
from shapely import from_wkt
from shapely.geometry import GeometryCollection, LineString, MultiLineString
from shapely.ops import linemerge, substring, unary_union

NVDB_BASE = "https://nvdbapiles.atlas.vegvesen.no"
SEGMENTERT_URL = f"{NVDB_BASE}/vegnett/api/v4/veglenkesekvenser/segmentert"

FYLAKODE_MR = 15
X_CLIENT_DEFAULT = "MRFK-asfalt-parseller-2026"

KD_MAX = 30

Geom = Union[LineString, MultiLineString]


# ---------------------------------------------------------------------------
# Dataklasse – tilpasset Bok1.xlsx-format
# Kolonner: veg | s/d (fra) | Start | s/d (til) | Stopp | Side
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ParsellRow:
    nr: int           # vegNr  (fra "veg"-kolonnen)
    fraS: int         # S-nummer fra fra-s/d
    fraDs: int        # D-nummer fra fra-s/d
    fraM: int         # Start-meter
    tilS: int         # S-nummer fra til-s/d
    tilDs: int        # D-nummer fra til-s/d
    tilM: int         # Stopp-meter
    side: str         # V eller H
    kildefil: str

    # Ingen rundkjøring-logikk i dette formatet; beholdes for kompatibilitet
    @property
    def is_roundabout(self) -> bool:
        return False

    @property
    def felt(self) -> int:
        if self.fraS == self.tilS and self.fraDs == self.tilDs:
            if self.fraM > self.tilM:
                return 2
        return 1


# ---------------------------------------------------------------------------
# Hjelpe-funksjoner
# ---------------------------------------------------------------------------
def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if not s or s.lower() in ("nan", "x", ""):
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _parse_sd(sd_str: str) -> Optional[Tuple[int, int]]:
    """
    Parser 'S11D1' -> (11, 1).
    Returnerer None ved ugyldig verdi.
    """
    m = re.fullmatch(r"S(\d+)D(\d+)", str(sd_str).strip(), re.IGNORECASE)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def pick_header_row(df_raw: pd.DataFrame, must: str = "veg") -> int:
    for i in range(min(50, len(df_raw))):
        row = df_raw.iloc[i].astype(str).tolist()
        if any(must.lower() == str(c).strip().lower() for c in row):
            return i
    return 0


# ---------------------------------------------------------------------------
# Les xlsx
# ---------------------------------------------------------------------------
def read_xlsx_rows(xlsx_path: Path) -> List[ParsellRow]:
    raw = pd.read_excel(xlsx_path, sheet_name=0, header=None)
    header_row = pick_header_row(raw, must="veg")
    df = pd.read_excel(xlsx_path, sheet_name=0, header=header_row)

    # Håndter duplikate kolonnenavn (to s/d-kolonner) ved å gi dem unike navn
    cols = list(df.columns)
    seen: Dict[str, int] = {}
    new_cols = []
    for c in cols:
        c_clean = str(c).strip()
        if c_clean in seen:
            seen[c_clean] += 1
            new_cols.append(f"{c_clean}_{seen[c_clean]}")
        else:
            seen[c_clean] = 0
            new_cols.append(c_clean)
    df.columns = new_cols

    # Forventede kolonner etter normalisering:
    #   veg | s/d | Start | s/d_1 | Stopp | Side
    required = ["veg", "s/d", "Start", "s/d.1", "Stopp", "Side"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Mangler kolonner i {xlsx_path.name}: {missing}\n"
            f"Faktiske kolonner: {list(df.columns)}"
        )

    rows: List[ParsellRow] = []
    for _, r in df.iterrows():
        nr = _as_int(r.get("veg"))
        fra_sd = _parse_sd(r.get("s/d", ""))
        fra_m = _as_int(r.get("Start"))
        til_sd = _parse_sd(r.get("s/d.1", ""))
        til_m = _as_int(r.get("Stopp"))
        side = str(r.get("Side", "")).strip().upper()

        # Hopp over ufullstendige rader
        if nr is None or fra_sd is None or fra_m is None:
            continue
        if til_sd is None or til_m is None:
            continue

        rows.append(
            ParsellRow(
                nr=nr,
                fraS=fra_sd[0],
                fraDs=fra_sd[1],
                fraM=fra_m,
                tilS=til_sd[0],
                tilDs=til_sd[1],
                tilM=til_m,
                side=side,
                kildefil=xlsx_path.name,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# NVDB API
# ---------------------------------------------------------------------------
def make_session(x_client: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": f"{x_client} (python requests)",
            "X-Client": x_client,
        }
    )
    return s


def nvdb_get_segmenter(
    session: requests.Session,
    vegsystemref: str,
    srid: int = 5973,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "vegsystemreferanse": vegsystemref,
        "fylke": str(FYLAKODE_MR),
        "srid": str(srid),
        "antall": "10000",
        "inkluderAntall": "false",
    }
    resp = session.get(SEGMENTERT_URL, params=params, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"{vegsystemref} -> HTTP {resp.status_code}: {resp.text}")
    data = resp.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "objekter" in data:
        return cast(List[Dict[str, Any]], data["objekter"])
    raise RuntimeError(f"{vegsystemref} -> Uventet responsformat")


def _extract_seg_meter(seg: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    vsr = seg.get("vegsystemreferanse", {}) or {}
    st = vsr.get("strekning", {}) or {}
    fra = st.get("fra_meter")
    til = st.get("til_meter")
    if fra is None or til is None:
        return None
    try:
        return float(fra), float(til)
    except Exception:
        return None


def _interval_normalized(a: int, b: int) -> Tuple[int, int]:
    return (a, b) if a <= b else (b, a)


def build_vsr_linear(nr: int, s: int, d: int, m_from: int, m_to: int) -> str:
    lo, hi = _interval_normalized(m_from, m_to)
    return f"FV{nr} S{s}D{d} m{lo}-{hi}"


def _sd_bounds(
    session: requests.Session, nr: int, s: int, d: int
) -> Tuple[float, float]:
    vsr = f"FV{nr} S{s}D{d} m0-99999999"
    segs = nvdb_get_segmenter(session, vsr)
    mins: List[float] = []
    maxs: List[float] = []
    for seg in segs:
        mm = _extract_seg_meter(seg)
        if mm is None:
            continue
        mins.append(mm[0])
        maxs.append(mm[1])
    if not mins or not maxs:
        raise RuntimeError(f"Ingen meter-bounds for {vsr}")
    return min(mins), max(maxs)


def build_delrefs(
    row: ParsellRow, session: requests.Session
) -> List[Tuple[str, float, float]]:
    """
    Returnerer liste av (vegsystemref, req_from, req_to).
    Håndterer S/D-kryssing automatisk.
    """
    if row.fraS == row.tilS and row.fraDs == row.tilDs:
        lo, hi = _interval_normalized(row.fraM, row.tilM)
        vsr = build_vsr_linear(row.nr, row.fraS, row.fraDs, lo, hi)
        return [(vsr, float(lo), float(hi))]

    # Kryssing mellom to S/D-seksjoner
    _, start_max = _sd_bounds(session, row.nr, row.fraS, row.fraDs)
    end_min, _ = _sd_bounds(session, row.nr, row.tilS, row.tilDs)

    lo1, hi1 = _interval_normalized(row.fraM, int(math.floor(start_max)))
    lo2, hi2 = _interval_normalized(int(math.ceil(end_min)), row.tilM)

    refs: List[Tuple[str, float, float]] = []
    if hi1 > lo1:
        refs.append(
            (build_vsr_linear(row.nr, row.fraS, row.fraDs, lo1, hi1), float(lo1), float(hi1))
        )
    if hi2 > lo2:
        refs.append(
            (build_vsr_linear(row.nr, row.tilS, row.tilDs, lo2, hi2), float(lo2), float(hi2))
        )
    return refs


# ---------------------------------------------------------------------------
# Geometri-hjelpe-funksjoner
# ---------------------------------------------------------------------------
def _as_lines(g: Any) -> List[LineString]:
    if g is None or getattr(g, "is_empty", False):
        return []
    if isinstance(g, LineString):
        return [g]
    if isinstance(g, MultiLineString):
        return list(g.geoms)
    if isinstance(g, GeometryCollection):
        return [x for x in g.geoms if isinstance(x, LineString)]
    return []


def _clip_lines_by_meter(
    lines: List[LineString],
    seg_from_m: float,
    seg_to_m: float,
    req_from_m: float,
    req_to_m: float,
) -> List[LineString]:
    lo = max(seg_from_m, req_from_m)
    hi = min(seg_to_m, req_to_m)
    if hi <= lo:
        return []
    denom = seg_to_m - seg_from_m
    if denom <= 0:
        return []

    out: List[LineString] = []
    for line in lines:
        total_len = line.length
        if total_len <= 0:
            continue
        f0 = (lo - seg_from_m) / denom
        f1 = (hi - seg_from_m) / denom
        d0 = max(0.0, min(total_len, f0 * total_len))
        d1 = max(0.0, min(total_len, f1 * total_len))
        if d1 <= d0:
            continue
        piece = substring(line, d0, d1, normalized=False)
        if isinstance(piece, LineString) and not piece.is_empty:
            out.append(piece)
    return out


def to_vegtrase_geometry(g: Geom) -> Geom:
    if isinstance(g, MultiLineString):
        merged = linemerge(g)
        if isinstance(merged, LineString):
            return merged
        lines = list(merged.geoms) if isinstance(merged, MultiLineString) else list(g.geoms)
        return max(lines, key=lambda x: x.length) if lines else g
    return g


# ---------------------------------------------------------------------------
# Hoved-geometri-funksjon
# ---------------------------------------------------------------------------
def row_to_geometry(
    session: requests.Session,
    row: ParsellRow,
    cache: Dict[str, List[Dict[str, Any]]],
    *,
    force_vegtrase: bool,
) -> Tuple[Optional[Geom], str, Optional[str], str]:
    """Returnerer: (geometry, Status, Feil, Delrefs)"""
    try:
        refs = build_delrefs(row, session)
    except Exception as e:
        return None, "FEIL", f"build_delrefs: {e}", ""

    used_refs: List[str] = []
    all_parts: List[LineString] = []

    for vsr, req_from, req_to in refs:
        used_refs.append(vsr)
        segs = cache.get(vsr)
        if segs is None:
            try:
                segs = nvdb_get_segmenter(session, vsr)
            except Exception as e:
                return None, "FEIL", str(e), " | ".join(used_refs)
            cache[vsr] = segs

        for seg in segs:
            w = (seg.get("geometri") or {}).get("wkt")
            if not w:
                continue
            try:
                geom_any = from_wkt(w)
            except Exception:
                continue
            lines = _as_lines(geom_any)
            if not lines:
                continue
            mm = _extract_seg_meter(seg)
            if mm is None:
                all_parts.extend(lines)
            else:
                all_parts.extend(
                    _clip_lines_by_meter(lines, mm[0], mm[1], req_from, req_to)
                )

    if not all_parts:
        return (
            None,
            "FEIL",
            "Ingen geometri etter klipp (sjekk meter/S/D)",
            " | ".join(used_refs),
        )

    u = unary_union(all_parts)
    lines_u = _as_lines(u)
    if not lines_u:
        return None, "FEIL", "Union ga ingen linjer", " | ".join(used_refs)

    geom_out: Geom = lines_u[0] if len(lines_u) == 1 else MultiLineString(lines_u)

    if force_vegtrase:
        geom_out = to_vegtrase_geometry(geom_out)

    return geom_out, "OK", None, " | ".join(used_refs)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(
        description="Hent geometri fra NVDB for parseller i Bok1.xlsx-format"
    )
    ap.add_argument("--xlsx", action="append", required=True,
                    help="Sti til xlsx-fil (kan gis flere ganger)")
    ap.add_argument("--out", required=True,
                    help="Output GeoPackage (.gpkg)")
    ap.add_argument("--layer", default="asfalt_parseller_fv_mr")
    ap.add_argument("--x-client", default=X_CLIENT_DEFAULT)
    ap.add_argument("--srid", type=int, default=5973)
    ap.add_argument("--vegtrase", action="store_true",
                    help="Tving vegtrase (lengste linje ved MultiLineString)")
    args = ap.parse_args()

    xlsx_files = [Path(p) for p in args.xlsx]
    out_path = Path(args.out)

    rows: List[ParsellRow] = []
    for x in xlsx_files:
        if not x.exists():
            print(f"Finner ikke fil: {x}", file=sys.stderr)
            return 2
        new_rows = read_xlsx_rows(x)
        print(f"  {x.name}: leste {len(new_rows)} rader")
        rows.extend(new_rows)

    if not rows:
        print("Ingen rader å prosessere.", file=sys.stderr)
        return 2

    session = make_session(args.x_client)
    cache: Dict[str, List[Dict[str, Any]]] = {}

    feats: List[Dict[str, Any]] = []
    ok = 0
    fe = 0

    for i, row in enumerate(rows, 1):
        print(
            f"[{i}/{len(rows)}] FV{row.nr} S{row.fraS}D{row.fraDs} m{row.fraM} "
            f"-> S{row.tilS}D{row.tilDs} m{row.tilM} ({row.side})",
            end=" ... ",
            flush=True,
        )
        geom, status, err, delrefs = row_to_geometry(
            session=session,
            row=row,
            cache=cache,
            force_vegtrase=bool(args.vegtrase),
        )
        print(status)

        if status == "OK":
            ok += 1
        else:
            fe += 1

        feats.append(
            {
                "geometry": geom,
                "VegNr": row.nr,
                "FraS": row.fraS,
                "FraDs": row.fraDs,
                "FraM": row.fraM,
                "TilS": row.tilS,
                "TilDs": row.tilDs,
                "TilM": row.tilM,
                "Side": row.side,
                "KildeFil": row.kildefil,
                "Felt": row.felt,
                "Delrefs": delrefs,
                "Status": status,
                "Feil": err,
            }
        )

    gdf = gpd.GeoDataFrame(feats, geometry="geometry", crs=f"EPSG:{args.srid}")
    gdf.to_file(out_path, layer=args.layer, driver="GPKG")

    print(f"\nSkrev {len(gdf)} rader til {out_path} (OK={ok}, FEIL={fe})")
    if fe:
        bad = gdf[gdf["Status"] != "OK"][
            ["VegNr", "FraS", "FraDs", "FraM", "TilS", "TilDs", "TilM", "Side", "Delrefs", "Feil"]
        ]
        with pd.option_context("display.max_colwidth", 180, "display.width", 240):
            print(bad.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
