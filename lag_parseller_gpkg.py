# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import pandas as pd
import requests

import geopandas as gpd
from shapely import from_wkt
from shapely.geometry import GeometryCollection, LineString, MultiLineString, Point
from shapely.ops import linemerge, substring, unary_union

NVDB_BASE = "https://nvdbapiles.atlas.vegvesen.no"
SEGMENTERT_URL = f"{NVDB_BASE}/vegnett/api/v4/veglenkesekvenser/segmentert"

FYLAKODE_MR = 15
X_CLIENT_DEFAULT = "MRFK-asfalt-parseller-2026"

KD_MAX = 30
DEFAULT_KD_NR = 1

Geom = Union[LineString, MultiLineString]


@dataclass(frozen=True)
class ParsellRow:
    kontrakt: str
    nr: int
    navn: str
    fraS: int
    fraDs: int
    fraM: int
    tilS: int
    tilDs: int
    tilM: int
    lengde: Optional[float]
    aadt: Optional[float]
    kildefil: str

    @property
    def is_roundabout(self) -> bool:
        return self.tilS == 0 and self.tilDs == 0 and self.tilM == 0

    @property
    def felt(self) -> int:
        if not self.is_roundabout and self.fraS == self.tilS and self.fraDs == self.tilDs:
            if self.fraM > self.tilM:
                return 2
        return 1


def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


def pick_header_row(df_raw: pd.DataFrame, must: str = "Kontrakt") -> int:
    for i in range(min(50, len(df_raw))):
        row = df_raw.iloc[i].astype(str).tolist()
        if any(must.lower() == str(c).strip().lower() for c in row):
            return i
    return 0


def read_xlsx_rows(xlsx_path: Path) -> List[ParsellRow]:
    raw = pd.read_excel(xlsx_path, sheet_name=0, header=None)
    header_row = pick_header_row(raw, must="Kontrakt")
    df = pd.read_excel(xlsx_path, sheet_name=0, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]

    required = [
        "Kontrakt",
        "Nr",
        "Navn",
        "FraS",
        "FraDs",
        "FraM",
        "TilS",
        "TilDs",
        "TilM",
        "Lengde",
        "ÅDT",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Mangler kolonner i {xlsx_path.name}: {missing}")

    rows: List[ParsellRow] = []
    for _, r in df.iterrows():
        nr = _as_int(r.get("Nr"))
        fraS = _as_int(r.get("FraS"))
        fraDs = _as_int(r.get("FraDs"))
        fraM = _as_int(r.get("FraM"))
        tilS = _as_int(r.get("TilS"))
        tilDs = _as_int(r.get("TilDs"))
        tilM = _as_int(r.get("TilM"))

        if nr is None or fraS is None or fraDs is None or fraM is None:
            continue
        if tilS is None or tilDs is None or tilM is None:
            continue

        rows.append(
            ParsellRow(
                kontrakt=str(r.get("Kontrakt", "")).strip(),
                nr=nr,
                navn=str(r.get("Navn", "")).strip(),
                fraS=fraS,
                fraDs=fraDs,
                fraM=fraM,
                tilS=tilS,
                tilDs=tilDs,
                tilM=tilM,
                lengde=_as_float(r.get("Lengde")),
                aadt=_as_float(r.get("ÅDT")),
                kildefil=xlsx_path.name,
            )
        )
    return rows


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
    # NB: Ingen topologiniva - API avviser parameteren
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


def build_vsr_roundabout(nr: int, s: int, d: int, anchor_m: int, kd: int, kd_len: int) -> str:
    kd_len = max(1, int(kd_len))
    return f"FV{nr} S{s}D{d} m{anchor_m} KD{kd} m0-{kd_len}"


def _kd_len_from_row(row: ParsellRow) -> int:
    if row.lengde and row.lengde > 0:
        return int(round(row.lengde))
    return 50


def _as_lines(g) -> List[LineString]:
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

    denom = (seg_to_m - seg_from_m)
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
        # Point/annet ignoreres
    return out


def to_vegtrase_geometry(g: Geom) -> Geom:
    """
    Reduser detaljnivå lokalt (siden API ikke støtter topologiniva):
    - linemerge (hvis mulig)
    - velg lengste linje
    """
    if isinstance(g, MultiLineString):
        merged = linemerge(g)
        if isinstance(merged, LineString):
            return merged
        if isinstance(merged, MultiLineString):
            g2 = merged
        else:
            g2 = g
        lines = list(g2.geoms)
        return max(lines, key=lambda x: x.length) if lines else g
    # LineString: allerede trase
    return g


def _sd_bounds(session: requests.Session, nr: int, s: int, d: int) -> Tuple[float, float]:
    """
    Finn min/max meter på en S/D ved å spørre et "bredt" intervall.
    Vi kan ikke spørre bare 'FV60 S5D1' på segmentert (gir 400 på noen),
    så vi bruker alltid m0-99999999.
    """
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


def build_delrefs(row: ParsellRow, session: requests.Session) -> List[Tuple[str, float, float, bool]]:
    """
    Returnerer liste av (vegsystemref, req_from, req_to, is_kd)
    """
    if row.is_roundabout:
        kd_len = _kd_len_from_row(row)
        out: List[Tuple[str, float, float, bool]] = []
        for kd in range(1, KD_MAX + 1):
            vsr = build_vsr_roundabout(row.nr, row.fraS, row.fraDs, row.fraM, kd, kd_len)
            out.append((vsr, 0.0, float(kd_len), True))
        return out

    if row.fraS == row.tilS and row.fraDs == row.tilDs:
        lo, hi = _interval_normalized(row.fraM, row.tilM)
        vsr = build_vsr_linear(row.nr, row.fraS, row.fraDs, lo, hi)
        return [(vsr, float(lo), float(hi), False)]

    # Kryssing S/D => split
    _, start_max = _sd_bounds(session, row.nr, row.fraS, row.fraDs)
    end_min, _ = _sd_bounds(session, row.nr, row.tilS, row.tilDs)

    lo1, hi1 = _interval_normalized(row.fraM, int(math.floor(start_max)))
    lo2, hi2 = _interval_normalized(int(math.ceil(end_min)), row.tilM)

    refs: List[Tuple[str, float, float, bool]] = []
    if hi1 > lo1:
        refs.append((build_vsr_linear(row.nr, row.fraS, row.fraDs, lo1, hi1), float(lo1), float(hi1), False))
    if hi2 > lo2:
        refs.append((build_vsr_linear(row.nr, row.tilS, row.tilDs, lo2, hi2), float(lo2), float(hi2), False))
    return refs


def row_to_geometry(
    session: requests.Session,
    row: ParsellRow,
    cache: Dict[str, List[Dict[str, Any]]],
    *,
    force_vegtrase: bool,
) -> Tuple[Optional[Geom], str, Optional[str], str]:
    """
    Returnerer: (geometry, Status, Feil, Delrefs)
    """
    refs = build_delrefs(row, session)

    # Rundkjøring: prøv KD-referanser til første som gir geometri
    if row.is_roundabout:
        kd_err_last: Optional[str] = None
        for vsr, req_from, req_to, _ in refs:
            try:
                segs = cache.get(vsr)
                if segs is None:
                    segs = nvdb_get_segmenter(session, vsr)
                    cache[vsr] = segs
            except Exception as e:
                kd_err_last = str(e)
                continue

            parts: List[LineString] = []
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
                    parts.extend(lines)
                else:
                    parts.extend(_clip_lines_by_meter(lines, mm[0], mm[1], req_from, req_to))

            if not parts:
                continue

            u = unary_union(parts)
            lines_u = _as_lines(u)
            if not lines_u:
                continue

            geom_out: Geom
            if len(lines_u) == 1:
                geom_out = lines_u[0]
            else:
                geom_out = MultiLineString(lines_u)

            if force_vegtrase:
                geom_out = to_vegtrase_geometry(geom_out)

            return geom_out, "OK", None, vsr

        return None, "FEIL", kd_err_last or "Ingen KD-referanse ga geometri", " | ".join([r[0] for r in refs[:3]])

    # Lineære parseller (inkl. S1->S2 split)
    used_refs: List[str] = []
    all_parts: List[LineString] = []

    for vsr, req_from, req_to, _ in refs:
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
                all_parts.extend(_clip_lines_by_meter(lines, mm[0], mm[1], req_from, req_to))

    if not all_parts:
        return None, "FEIL", "Ingen geometri etter klipp (sjekk meter/S/D)", " | ".join(used_refs)

    u = unary_union(all_parts)
    lines_u = _as_lines(u)
    if not lines_u:
        return None, "FEIL", "Union ga ingen linjer", " | ".join(used_refs)

    geom_out: Geom
    if len(lines_u) == 1:
        geom_out = lines_u[0]
    else:
        geom_out = MultiLineString(lines_u)

    if force_vegtrase:
        geom_out = to_vegtrase_geometry(geom_out)

    return geom_out, "OK", None, " | ".join(used_refs)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", action="append", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--layer", default="asfalt_parseller_fv_mr")
    ap.add_argument("--x-client", default=X_CLIENT_DEFAULT)
    ap.add_argument("--srid", type=int, default=5973)
    ap.add_argument("--vegtrase", action="store_true")
    args = ap.parse_args()

    xlsx_files = [Path(p) for p in args.xlsx]
    out_path = Path(args.out)

    rows: List[ParsellRow] = []
    for x in xlsx_files:
        if not x.exists():
            print(f"Finner ikke fil: {x}", file=sys.stderr)
            return 2
        rows.extend(read_xlsx_rows(x))

    if not rows:
        print("Ingen rader å prosessere.", file=sys.stderr)
        return 2

    session = make_session(args.x_client)
    cache: Dict[str, List[Dict[str, Any]]] = {}

    feats: List[Dict[str, Any]] = []
    ok = 0
    fe = 0

    for row in rows:
        geom, status, err, delrefs = row_to_geometry(
            session=session,
            row=row,
            cache=cache,
            force_vegtrase=bool(args.vegtrase),
        )

        if status == "OK":
            ok += 1
        else:
            fe += 1

        feats.append(
            {
                "geometry": geom,
                "Kontrakt": row.kontrakt,
                "Nr": row.nr,
                "Navn": row.navn,
                "FraS": row.fraS,
                "FraDs": row.fraDs,
                "FraM": row.fraM,
                "TilS": row.tilS,
                "TilDs": row.tilDs,
                "TilM": row.tilM,
                "Lengde": row.lengde,
                "ÅDT": row.aadt,
                "KildeFil": row.kildefil,
                "Felt": row.felt,
                "Delrefs": delrefs,
                "Status": status,
                "Feil": err,
            }
        )

    gdf = gpd.GeoDataFrame(feats, geometry="geometry", crs=f"EPSG:{args.srid}")
    gdf.to_file(out_path, layer=args.layer, driver="GPKG")

    print(f"Skrev {len(gdf)} rader til {out_path} (OK={ok}, FEIL={fe})")
    if fe:
        bad = gdf[gdf["Status"] != "OK"][["Kontrakt", "Nr", "Navn", "Delrefs", "Feil"]]
        with pd.option_context("display.max_colwidth", 180, "display.width", 240):
            print(bad.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
