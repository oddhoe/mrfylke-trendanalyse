# -*- coding: utf-8 -*-
"""
Bygg GeoPackage (.gpkg) fra rå NVDB-JSON sider (page_*.json) fra --save-raw-dir.

Fikser koordinatrekkefølge:
- NVDB-WKT (SRID=4326) ser hos deg ut til å komme som POINT(lat lon)
- GeoPackage/Shapely forventer POINT(lon lat)
=> vi bytter derfor om før vi lager Point().

Kjør:
  python json_pages_to_gpkg.py --raw_dir raw_592 --out_gpkg FWD_592_MR.gpkg

Krever:
  pip install geopandas shapely
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
from shapely.geometry import Point


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None
    return None


def parse_date(x: Any) -> Optional[dt.date]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def wkt_point_xy(wkt: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Leser første to tall fra WKT POINT.
    Returnerer (x, y) slik de står i teksten.
    """
    m = re.search(r"POINT(?: Z)?\s*\(\s*([-\d\.]+)\s+([-\d\.]+)", wkt or "")
    if not m:
        return None, None
    x = safe_float(m.group(1))
    y = safe_float(m.group(2))
    return x, y


def egenskaper_map(obj: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for e in obj.get("egenskaper") or []:
        navn = e.get("navn")
        if not navn:
            continue
        if "verdi" in e:
            out[navn] = e.get("verdi")
        elif "verdiTekst" in e:
            out[navn] = e.get("verdiTekst")
        else:
            out[navn] = e
    return out


def find_fv_and_meter(obj: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    lok = obj.get("lokasjon") or {}
    vsrs = lok.get("vegsystemreferanser") or []
    for vsr in vsrs:
        vs = vsr.get("vegsystem") or {}
        if vs.get("vegkategori") != "F":
            continue
        nummer = vs.get("nummer")
        if not isinstance(nummer, int):
            continue
        strek = vsr.get("strekning") or {}
        meter = safe_float(strek.get("meter"))
        return f"FV{nummer}", meter
    return None, None


def pick_tons(em: Dict[str, Any]) -> Optional[float]:
    # Observasjoner fra din debug: "Bæreevne" og "Bæreevne, temperaturkorrigert"
    for k in ("Bæreevne, temperaturkorrigert", "Bæreevne"):
        if k in em:
            v = safe_float(em.get(k))
            if v is not None:
                return v
    return None


def looks_like_norway(lon: Optional[float], lat: Optional[float]) -> bool:
    if lon is None or lat is None:
        return False
    # Grov sjekk for Norge-ish (inkl. litt slingringsmonn)
    return (0.0 <= lon <= 32.0) and (57.0 <= lat <= 72.0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw_dir", required=True, help="Mappe med page_*.json")
    ap.add_argument("--out_gpkg", required=True, help="Output .gpkg")
    ap.add_argument("--layer", default="FWD_592_MR_2017_2026", help="Lag-navn i gpkg")
    ap.add_argument("--date_from", default="2017-01-01", help="YYYY-MM-DD")
    ap.add_argument("--fylke", type=int, default=15, help="MR=15")
    ap.add_argument("--assume_nvdb_lat_lon", action="store_true", default=True)
    args = ap.parse_args()

    date_from = parse_date(args.date_from)
    if not date_from:
        raise SystemExit("Ugyldig --date_from (må være YYYY-MM-DD)")

    paths = sorted(glob.glob(os.path.join(args.raw_dir, "page_*.json")))
    if not paths:
        raise SystemExit("Fant ingen page_*.json i raw_dir")

    rows: List[Dict[str, Any]] = []
    total_candidates = 0
    norway_ok = 0

    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)

        for obj in data.get("objekter") or []:
            # Filter på fylke=15 (lokasjon.fylker er liste)
            lok = obj.get("lokasjon") or {}
            fylker = lok.get("fylker") or []
            if args.fylke not in fylker:
                continue

            em = egenskaper_map(obj)

            mdate = parse_date(em.get("Måledato"))
            if mdate is None or mdate < date_from:
                continue

            fv, meter = find_fv_and_meter(obj)
            if fv is None:
                continue

            tons = pick_tons(em)

            geo = obj.get("geometri") or {}
            wkt = geo.get("wkt") or ""
            srid = geo.get("srid")

            # Vi forventer 4326 for lon/lat i grader.
            if srid != 4326:
                continue

            x, y = wkt_point_xy(wkt)
            if x is None or y is None:
                continue

            # FIKS: NVDB-WKT ser hos deg ut som POINT(lat lon).
            # Derfor: lat = x, lon = y
            lat = x
            lon = y

            total_candidates += 1
            if looks_like_norway(lon, lat):
                norway_ok += 1

            rows.append(
                {
                    "obj_id": int(obj.get("id")),
                    "fv": fv,
                    "meter": meter,
                    "tons": tons,
                    "measure_date": mdate.isoformat(),
                    "srid": srid,
                    "geometry": Point(lon, lat),  # GeoPackage: X=lon, Y=lat
                }
            )

    if not rows:
        raise SystemExit("Ingen rader etter filtrering. Sjekk input/filtre/SRID.")

    ratio = (norway_ok / total_candidates) if total_candidates else 0.0
    if total_candidates and ratio < 0.80:
        print(
            "ADVARSEL: Kun {:.1%} av punktene ser ut til å ligge i Norge (grov sjekk). "
            "Hvis kartet fortsatt er tomt/feil sted, må vi revurdere koordinatrekkefølge."
            .format(ratio)
        )

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

    out_gpkg = os.path.abspath(args.out_gpkg)
    gdf.to_file(out_gpkg, layer=args.layer, driver="GPKG")

    print(f"Skrev {len(gdf)} punkter til {out_gpkg} (layer={args.layer})")
    print(f"Sanity: Norge-ish {norway_ok}/{total_candidates} = {ratio:.1%}")


if __name__ == "__main__":
    main()
