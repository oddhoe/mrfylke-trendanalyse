# 06_hent_ekstra_info.py
from __future__ import annotations

from typing import Optional
import os
import requests
import arcpy

from config import GDB, FYLKE, SRID
from naming import fc

HOYDE_OBJEKT_ID = 591
MAX_HOYDE_M = 4.5
OUT_FC = fc(GDB, "Hoydebegrensning_LAV")
HEADERS = {"X-Client": "nvdb_script", "Accept": "application/vnd.vegvesen.nvdb-v3-rev1+json"}
NVDB_API = "https://nvdbapiles.atlas.vegvesen.no"
VEGOBJ_API = f"{NVDB_API}/vegobjekter/api/v4"

arcpy.env.overwriteOutput = True

def log(msg: str) -> None:
    print(msg)

def iter_paged(url: str, params: dict):
    offset: Optional[str] = None
    while True:
        p = dict(params); 
        if offset: p["start"] = offset
        r = requests.get(url, params=p, headers=HEADERS, timeout=30)
        r.raise_for_status()
        d = r.json(); objs = d.get("objekter", [])
        if not objs: break
        for o in objs: yield o
        nxt = d.get("metadata", {}).get("neste")
        if not nxt: break
        offset = nxt.get("start")

def to_geometry(geom: dict) -> Optional[arcpy.Geometry]:
    if not geom or "wkt" not in geom: return None
    try:
        return arcpy.FromWKT(geom["wkt"], arcpy.SpatialReference(SRID))
    except Exception:
        return None

def extract_hoyde(egenskaper: list) -> Optional[float]:
    beregnet = skiltet = None
    for e in egenskaper:
        eid = e.get("id"); val = e.get("verdi")
        if val is None: continue
        if eid == 10247:  # Beregnet høyde
            try: beregnet = float(val)
            except ValueError: pass
        elif eid == 5277: # Skilta høyde
            try: skiltet = float(val)
            except ValueError: pass
    return beregnet if beregnet is not None else skiltet

def create_fc() -> None:
    if arcpy.Exists(OUT_FC): arcpy.management.Delete(OUT_FC)
    arcpy.management.CreateFeatureclass(
        out_path=os.path.dirname(OUT_FC),
        out_name=os.path.basename(OUT_FC),
        geometry_type="POLYLINE",
        spatial_reference=SRID,
    )
    arcpy.management.AddField(OUT_FC, "VEGLENKESEKV_ID", "LONG")
    arcpy.management.AddField(OUT_FC, "STARTPOS", "DOUBLE")
    arcpy.management.AddField(OUT_FC, "SLUTTPOS", "DOUBLE")
    arcpy.management.AddField(OUT_FC, "MIN_HOYDE", "DOUBLE")
    arcpy.management.AddField(OUT_FC, "KILDE", "TEXT", field_length=30)

def main() -> None:
    log("Henter høydebegrensninger (objekt 591)…")
    create_fc()

    url = f"{VEGOBJ_API}/vegobjekter/{HOYDE_OBJEKT_ID}"
    params = {"fylke": FYLKE, "antall": 1000, "inkluder": "egenskaper,lokasjon,geometri"}

    count = 0
    with arcpy.da.InsertCursor(
        OUT_FC, ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "MIN_HOYDE", "KILDE"]
    ) as cur:
        for obj in iter_paged(url, params):
            hoyde = extract_hoyde(obj.get("egenskaper", []))
            if hoyde is None or hoyde >= MAX_HOYDE_M:
                continue
            geom = to_geometry(obj.get("geometri"))
            if geom is None:
                continue
            for s in obj.get("lokasjon", {}).get("stedfestinger", []):
                vid = s.get("veglenkesekvensid")
                if vid is None:
                    continue
                cur.insertRow((
                    geom,
                    int(vid),
                    float(s.get("startposisjon", 0.0)),
                    float(s.get("sluttposisjon", 0.0)),
                    hoyde,
                    "Beregnet/Skilta"
                ))
                count += 1
    log(f"✅ Ferdig: {count} høydebegrensninger < {MAX_HOYDE_M} m")

if __name__ == "__main__":
    main()