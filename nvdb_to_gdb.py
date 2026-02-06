# nvdb_to_gdb.py - ENDELIG VERSJON (NATT-EDITION)
# - Oppretter GDB korrekt
# - Henter ALLE stedfestinger (ingen hull)
# - Parser BK smart (ingen 8-tonns feil)

import os
import requests
import arcpy
import re

# -------------------------
# KONFIG
# -------------------------
FYLKE = 15
SRID = 5973
PAGE_SIZE = 1000
NVDB_API = "https://nvdbapiles.atlas.vegvesen.no"
VEGNETT_API = f"{NVDB_API}/vegnett/api/v4"
VEGOBJ_API = f"{NVDB_API}/vegobjekter/api/v4"
OUT_GDB = r"D:\Conda\Flaskehalser\gdb\nvdb_radata.gdb"

# Blacklist (Bugge bru)
BLACKLIST_BRUER = [] 

arcpy.env.overwriteOutput = True

def log(msg): print(msg)

def iter_paged(url, params, headers):
    offset = None
    while True:
        p = dict(params)
        if offset: p["start"] = offset
        r = requests.get(url, params=p, headers=headers)
        if r.status_code != 200: break
        data = r.json()
        objs = data.get("objekter", [])
        if not objs: break
        for o in objs: yield o
        neste = data.get("metadata", {}).get("neste")
        if not neste: break
        offset = neste.get("start")

def to_geometry(geom):
    if not geom or "wkt" not in geom: return None
    try: return arcpy.FromWKT(geom["wkt"], arcpy.SpatialReference(SRID))
    except: return None

def create_gdb(path):
    folder, name = os.path.split(path)
    if not os.path.exists(folder):
        os.makedirs(folder)
    if not arcpy.Exists(path):
        log(f"Oppretter GDB: {path}")
        arcpy.management.CreateFileGDB(folder, name)

def create_fc(gdb, name, geom_type, extra_fields=[]):
    fc = os.path.join(gdb, name)
    if arcpy.Exists(fc): arcpy.management.Delete(fc)
    arcpy.management.CreateFeatureclass(gdb, name, geom_type, spatial_reference=SRID)
    arcpy.management.AddField(fc, "VEGLENKESEKV_ID", "LONG")
    arcpy.management.AddField(fc, "STARTPOS", "DOUBLE")
    arcpy.management.AddField(fc, "SLUTTPOS", "DOUBLE")
    for f in extra_fields:
        arcpy.management.AddField(fc, f[0], f[1], field_length=f[2] if len(f)>2 else None)
    return fc

# -------------------------
# 1. VEGNETT
# -------------------------
def hent_vegnett(gdb):
    log("Henter vegnett (med posisjon)...")
    fc = create_fc(gdb, "Vegnett", "POLYLINE", [("VEGKATEGORI", "TEXT", 1), ("VEGNUMMER", "LONG")])
    
    url = f"{VEGNETT_API}/veglenkesekvenser/segmentert"
    params = {"fylke": FYLKE, "vegsystemreferanse": "F", "antall": 5000, "inkluderAntall": "false"}
    headers = {"X-Client": "nvdb_script", "Accept": "application/vnd.vegvesen.nvdb-v3-rev1+json"}
    
    cnt = 0
    with arcpy.da.InsertCursor(fc, ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "VEGKATEGORI", "VEGNUMMER"]) as cur:
        for seg in iter_paged(url, params, headers):
            vr = seg.get("vegsystemreferanse", {})
            if vr.get("strekning", {}).get("trafikantgruppe") != "K": continue
            
            geom = to_geometry(seg.get("geometri"))
            if not geom: continue
            
            cur.insertRow((
                geom, 
                int(seg["veglenkesekvensid"]),
                float(seg.get("startposisjon", 0)),
                float(seg.get("sluttposisjon", 0)),
                vr.get("vegsystem", {}).get("vegkategori"),
                vr.get("vegsystem", {}).get("nummer")
            ))
            cnt += 1
    log(f"Vegnett ferdig: {cnt}")

# -------------------------
# 2. BRUER
# -------------------------
def hent_bruer(gdb):
    log("Henter bruer (med posisjon)...")
    fields = [("BRU_ID", "LONG"), ("BRU_NAVN", "TEXT", 100), ("TILLATT_TONN", "LONG"), ("BRUKSLAST", "TEXT", 50)]
    fc = create_fc(gdb, "Bruer", "POLYLINE", fields)
    
    url = f"{VEGOBJ_API}/vegobjekter/60"
    params = {"fylke": FYLKE, "vegsystemreferanse": "F", "antall": 1000, "inkluder": "egenskaper,lokasjon,geometri"}
    headers = {"X-Client": "nvdb_script", "Accept": "application/vnd.vegvesen.nvdb-v3-rev1+json"}
    
    cnt = 0
    with arcpy.da.InsertCursor(fc, ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "BRU_ID", "BRU_NAVN", "TILLATT_TONN", "BRUKSLAST"]) as cur:
        for o in iter_paged(url, params, headers):
            if int(o["id"]) in BLACKLIST_BRUER: continue
            
            if not any(v.get("strekning", {}).get("trafikantgruppe") == "K" for v in o.get("lokasjon", {}).get("vegsystemreferanser", [])):
                continue
                
            navn = None
            tillatt = None
            brukslast = None
            er_vegbru = False
            er_trafikkert = False
            
            for e in o.get("egenskaper", []):
                eid = e["id"]
                val = str(e["verdi"]).strip()
                
                if eid == 1080: navn = val
                elif eid == 1263 and val == "Vegbru": er_vegbru = True
                elif eid == 11317 and val == "Trafikkert": er_trafikkert = True
                elif eid == 12653:
                    brukslast = val
                    m = re.search(r"/(\d+)", val)
                    if m: tillatt = int(m.group(1))
            
            if not (er_vegbru and er_trafikkert): continue
            
            geom = to_geometry(o.get("geometri"))
            if not geom: continue
            if geom.type == "polygon": geom = geom.boundary()
            
            # Lagre ALLE stedfestinger
            for s in o.get("lokasjon", {}).get("stedfestinger", []):
                if s.get("veglenkesekvensid"):
                    cur.insertRow((
                        geom,
                        int(s["veglenkesekvensid"]),
                        float(s.get("startposisjon", 0)),
                        float(s.get("sluttposisjon", 0)),
                        int(o["id"]),
                        navn,
                        tillatt,
                        brukslast
                    ))
                    cnt += 1
    log(f"Bruer ferdig: {cnt}")

# -------------------------
# 3. BRUKSKLASSE
# -------------------------
def hent_bruksklasse(gdb):
    log("Henter bruksklasser (smart parsing)...")
    fields = [("BK_VERDI", "LONG"), ("BK_TEKST", "TEXT", 50)]
    fc = create_fc(gdb, "Bruksklasse", "POLYLINE", fields)
    
    url = f"{VEGOBJ_API}/vegobjekter/900"
    params = {"fylke": FYLKE, "vegsystemreferanse": "F", "antall": 1000, "inkluder": "egenskaper,lokasjon,geometri"}
    headers = {"X-Client": "nvdb_script", "Accept": "application/vnd.vegvesen.nvdb-v3-rev1+json"}
    
    cnt = 0
    with arcpy.da.InsertCursor(fc, ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "BK_VERDI", "BK_TEKST"]) as cur:
        for o in iter_paged(url, params, headers):
            val = None
            tekst = None
            for e in o.get("egenskaper", []):
                if e["id"] == 10897:
                    tekst = str(e["verdi"])
                    # Smart parsing
                    m = re.search(r"/\s*(\d+)", tekst)
                    if m: val = int(m.group(1))
                    else:
                        m = re.search(r"(\d+)\s*tonn", tekst, re.IGNORECASE)
                        if m: val = int(m.group(1))
                        else:
                            tall = [int(t) for t in re.findall(r"(\d+)", tekst)]
                            if tall: val = max(tall)
            
            if not val: continue
            if val < 20 and tekst: 
                 tall = [int(t) for t in re.findall(r"(\d+)", tekst)]
                 if tall: val = max(tall)

            geom = to_geometry(o.get("geometri"))
            if not geom: continue
            
            # Lagre ALLE stedfestinger
            for s in o.get("lokasjon", {}).get("stedfestinger", []):
                if s.get("veglenkesekvensid"):
                    cur.insertRow((
                        geom,
                        int(s["veglenkesekvensid"]),
                        float(s.get("startposisjon", 0)),
                        float(s.get("sluttposisjon", 0)),
                        val,
                        tekst
                    ))
                    cnt += 1
    log(f"Bruksklasser ferdig: {cnt}")

if __name__ == "__main__":
    create_gdb(OUT_GDB)
    hent_vegnett(OUT_GDB)
    hent_bruer(OUT_GDB)
    hent_bruksklasse(OUT_GDB)
    log(f"✓ NVDB → GDB ferdig: {OUT_GDB}")