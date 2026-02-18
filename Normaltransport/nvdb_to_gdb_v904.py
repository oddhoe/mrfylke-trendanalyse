# -*- coding: utf-8 -*-
"""
nvdb_to_gdb_v904.py

Formål
------
Hent FV-vegnett (fylke 15) + relevante vegobjekter til FileGDB:
- Vegnett (segmentert) med VEGLENKESEKV_ID + start/sluttpos
- Bruer (objtype 60) med tillatt totalvekt (fra "Brukslast" / tall i tekst)
- Bruksklasse, normaltransport (objtype 904) med:
    * BK_VERDI (tolket tonn der mulig)
    * BK_TEKST (original tekst/kode)
    * MAKS_LENGDE (meter, tolket der mulig)
- Høydebegrensning (objtype 591) som punkt (skiltet høyde)

Merk
----
NVDB har mange varianter av egenskapsnavn/innhold over tid. Dette skriptet forsøker
å være robust ved å parse basert på egenskapsNAVN/tekst (ikke hardkodede id-er),
slik at små katalogendringer ikke knekker alt.
"""

from __future__ import annotations

import os
import re

import arcpy
import requests

arcpy.env.overwriteOutput = True

# -------------------------
# KONFIG
# -------------------------
FYLKE = 15
SRID = 5973

NVDB_API = "https://nvdbapiles.atlas.vegvesen.no"
VEGNETT_API = f"{NVDB_API}/vegnett/api/v4"
VEGOBJ_API  = f"{NVDB_API}/vegobjekter/api/v4"

OUT_GDB = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"

# Filtre
VEGSYSTEMREF = "F"      # Fylkesveg (F)
TRAFIKANTGRP = "K"      # Kjørefelt

# Objekttyper
OBJ_BRU = 60
OBJ_BK  = 904   # Bruksklasse, normaltransport
OBJ_HOY = 591   # Høydebegrensning

HEADERS = {
    "X-Client": "mrfk_flaskehalsanalyse",
    "Accept": "application/vnd.vegvesen.nvdb-v3+json",
}

# -------------------------
# HJELP
# -------------------------
def log(msg: str) -> None:
    print(msg)

def iter_paged(url: str, params: dict) :
    """Generator for paginert NVDB v4 (metadata.neste.href)."""
    next_url = url
    next_params = dict(params)
    while next_url:
        r = requests.get(next_url, params=next_params, headers=HEADERS, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} for {next_url}: {r.text[:200]}")
        data = r.json()
        objs = data.get("objekter", [])
        for o in objs:
            yield o
        nxt = data.get("metadata", {}).get("neste", {}).get("href")
        next_url = nxt
        next_params = {}  # neste.href er ferdig query-string

def to_geometry(geom: dict):
    if not geom:
        return None
    wkt = geom.get("wkt")
    if not wkt:
        return None
    try:
        return arcpy.FromWKT(wkt, arcpy.SpatialReference(SRID))
    except Exception:
        return None

def create_gdb(path: str) -> None:
    folder, name = os.path.split(path)
    if not os.path.exists(folder):
        os.makedirs(folder)
    if not arcpy.Exists(path):
        log(f"Oppretter GDB: {path}")
        arcpy.management.CreateFileGDB(folder, name)

def create_fc(gdb: str, name: str, geom_type: str, extra_fields: list[tuple]) -> str:
    fc = os.path.join(gdb, name)
    if arcpy.Exists(fc):
        arcpy.management.Delete(fc)
    arcpy.management.CreateFeatureclass(gdb, name, geom_type, spatial_reference=SRID)
    arcpy.management.AddField(fc, "VEGLENKESEKV_ID", "LONG")
    arcpy.management.AddField(fc, "STARTPOS", "DOUBLE")
    arcpy.management.AddField(fc, "SLUTTPOS", "DOUBLE")
    for f in extra_fields:
        # f = (name, type, length?)
        if len(f) == 2:
            arcpy.management.AddField(fc, f[0], f[1])
        else:
            arcpy.management.AddField(fc, f[0], f[1], field_length=f[2])
    return fc

_num_re = re.compile(r"(\d+(?:[.,]\d+)?)")

def parse_float_any(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    m = _num_re.search(s)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))

def parse_tonn_from_text(s: str | None) -> int | None:
    """Trekk ut tonnverdi fra tekst (typisk BK10/60, 12/65, "60 tonn")."""
    if not s:
        return None
    # Typisk "BK10/60", "12/65", "BK10/42", "Brukslast 10/60"
    m = re.search(r"/\s*(\d+)", s)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*tonn", s, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # fallback: største heltall i teksten
    nums = [int(n) for n in re.findall(r"(\d+)", s)]
    return max(nums) if nums else None

def pick_property(egenskaper, name_contains: list[str]):
    """Finn første egenskap hvor navn matcher en av substrings (case-insensitive)."""
    if not egenskaper:
        return None
    for e in egenskaper:
        navn = (e.get("navn") or "").lower()
        for sub in name_contains:
            if sub.lower() in navn:
                return e
    return None


def safe_insert(cur, row, *, err_prefix: str, err_counter: dict, max_print: int = 10) -> None:
    """InsertRow med robust feilhåndtering slik at ett dårlig objekt ikke stopper hele kjøringen."""
    try:
        cur.insertRow(row)
    except Exception as e:
        err_counter["n"] += 1
        if err_counter["n"] <= max_print:
            log(f"{err_prefix}: {e}")

# -------------------------
# 1. VEGNETT
# -------------------------
def hent_vegnett(gdb: str) -> str:
    log("Henter vegnett (FV, segmentert, med posisjon)...")
    fc = create_fc(gdb, "Vegnett", "POLYLINE", [("VEGKATEGORI", "TEXT", 1), ("VEGNUMMER", "LONG")])

    url = f"{VEGNETT_API}/veglenkesekvenser/segmentert"
    params = {
        "fylke": FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall": 5000,
        "inkluderAntall": "false",
        "srid": SRID,
    }

    cnt = 0
    cols = ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "VEGKATEGORI", "VEGNUMMER"]
    with arcpy.da.InsertCursor(fc, cols) as cur:
        for seg in iter_paged(url, params):
            vr = seg.get("vegsystemreferanse", {})
            if vr.get("strekning", {}).get("trafikantgruppe") != TRAFIKANTGRP:
                continue

            geom = to_geometry(seg.get("geometri"))
            if not geom:
                continue

            cur.insertRow((
                geom,
                int(seg["veglenkesekvensid"]),
                float(seg.get("startposisjon", 0.0)),
                float(seg.get("sluttposisjon", 0.0)),
                vr.get("vegsystem", {}).get("vegkategori"),
                vr.get("vegsystem", {}).get("nummer"),
            ))
            cnt += 1

    log(f"Vegnett ferdig: {cnt}")
    return fc

# -------------------------
# 2. BRUER (60)
# -------------------------
def hent_bruer(gdb: str) -> str:
    log("Henter bruer (60) med posisjon...")
    fields = [
        ("BRU_ID", "LONG"),
        ("BRU_NAVN", "TEXT", 120),
        ("TILLATT_TONN", "LONG"),
        ("BRUKSLAST", "TEXT", 80),
    ]
    fc = create_fc(gdb, "Bruer", "POLYLINE", fields)

    url = f"{VEGOBJ_API}/vegobjekter/{OBJ_BRU}"
    params = {
        "fylke": FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall": 1000,
        "inkluder": "egenskaper,lokasjon,geometri",
        "srid": SRID,
        "alle_versjoner": "false",
    }

    cnt = 0
    cols = ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "BRU_ID", "BRU_NAVN", "TILLATT_TONN", "BRUKSLAST"]
    err = {"n": 0}
    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(url, params):
            # Trafikantgruppe-filter (robust)
            if not any(v.get("strekning", {}).get("trafikantgruppe") == TRAFIKANTGRP
                       for v in o.get("lokasjon", {}).get("vegsystemreferanser", [])):
                continue

            navn = None
            brukslast = None
            tillatt = None
            er_vegbru = None
            er_trafikkert = None

            for e in o.get("egenskaper", []):
                enavn = (e.get("navn") or "").lower()
                val = e.get("verdi")
                if "navn" in enavn and navn is None:
                    navn = str(val).strip() if val is not None else None
                # Mange bruobjekter har "Bru type" og "Trafikkstatus" – bruk hvis de finnes.
                if "bru type" in enavn or enavn.endswith("brutype"):
                    er_vegbru = (str(val).strip().lower() == "vegbru") if val is not None else er_vegbru
                if "trafikk" in enavn and "status" in enavn:
                    er_trafikkert = (str(val).strip().lower() == "trafikkert") if val is not None else er_trafikkert
                # Brukslast-feltet inneholder ofte ".../60"
                if "brukslast" in enavn:
                    brukslast = str(val).strip() if val is not None else None
                    t = parse_tonn_from_text(brukslast)
                    if t is not None:
                        tillatt = t
                # Noen katalogvarianter bruker "tillatt totalvekt"-aktig navn
                if tillatt is None and ("tillatt" in enavn and "tonn" in enavn):
                    t = parse_tonn_from_text(str(val) if val is not None else None)
                    if t is not None:
                        tillatt = t

            # Hvis katalogen gir oss flagg for type/status, filtrer hardt for å unngå "rare" bruer
            if er_vegbru is False:
                continue
            if er_trafikkert is False:
                continue

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue
            if geom.type == "polygon":
                geom = geom.boundary()

            for s in o.get("lokasjon", {}).get("stedfestinger", []):
                if not s.get("veglenkesekvensid"):
                    continue
                safe_insert(
                    cur,
                    (
                        geom,
                        int(s["veglenkesekvensid"]),
                        float(s.get("startposisjon", 0.0)),
                        float(s.get("sluttposisjon", 0.0)),
                        int(o["id"]),
                        navn,
                        tillatt,
                        brukslast,
                    ),
                    err_prefix=f"[bru id={o.get('id')} stedfesting={s.get('veglenkesekvensid')}] insert-feil",
                    err_counter=err,
                )
                cnt += 1

    log(f"Bruer ferdig: {cnt}")
    if err["n"]:
        log(f"⚠️ Advarsel: {err['n']} bru-rader ble hoppet over pga insert-feil (se første linjer over).")
    return fc

# -------------------------
# 3. BRUKSKLASSE NORMALTRANSPORT (904)
# -------------------------
def hent_bruksklasse_904(gdb: str) -> str:
    log("Henter bruksklasse normaltransport (904) med posisjon...")
    fields = [
        ("BK_VERDI", "LONG"),
        ("BK_TEKST", "TEXT", 120),
        ("MAKS_LENGDE", "DOUBLE"),
    ]
    fc = create_fc(gdb, "Bruksklasse_904", "POLYLINE", fields)

    url = f"{VEGOBJ_API}/vegobjekter/{OBJ_BK}"
    params = {
        "fylke": FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall": 1000,
        "inkluder": "egenskaper,lokasjon,geometri",
        "srid": SRID,
        "alle_versjoner": "false",
    }

    cnt = 0
    cols = ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "BK_VERDI", "BK_TEKST", "MAKS_LENGDE"]
    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(url, params):
            eg = o.get("egenskaper", []) or []

            # Forsøk å finne relevante egenskaper ved navn
            e_bk = pick_property(eg, ["bruksklasse", "bk", "helår", "vinter"])
            e_len = pick_property(eg, ["vogntoglengde", "lengde"])

            bk_text = str(e_bk.get("verdi")).strip() if e_bk and e_bk.get("verdi") is not None else None
            bk_val = parse_tonn_from_text(bk_text) if bk_text else None

            maks_len = None
            if e_len and e_len.get("verdi") is not None:
                maks_len = parse_float_any(e_len.get("verdi"))

            # Geometri + stedfestinger
            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue

            for s in o.get("lokasjon", {}).get("stedfestinger", []):
                if not s.get("veglenkesekvensid"):
                    continue
                cur.insertRow((
                    geom,
                    int(s["veglenkesekvensid"]),
                    float(s.get("startposisjon", 0.0)),
                    float(s.get("sluttposisjon", 0.0)),
                    bk_val,
                    bk_text,
                    maks_len,
                ))
                cnt += 1

    log(f"Bruksklasse 904 ferdig: {cnt}")
    return fc

# -------------------------
# 4. HØYDEBEGRENSNING (591)
# -------------------------
def hent_hoydebegrensning(gdb: str) -> str:
    log("Henter høydebegrensning (591) med posisjon...")
    fields = [
        ("NVDB_ID", "LONG"),
        ("SKILTET_HOYDE", "DOUBLE"),
        ("TYPE_HINDER", "TEXT", 60),
    ]
    fc = create_fc(gdb, "Hoydebegrensning_591", "POINT", fields)

    url = f"{VEGOBJ_API}/vegobjekter/{OBJ_HOY}"
    params = {
        "fylke": FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall": 1000,
        "inkluder": "egenskaper,lokasjon,geometri",
        "srid": SRID,
        "alle_versjoner": "false",
    }

    cnt = 0
    cols = ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "NVDB_ID", "SKILTET_HOYDE", "TYPE_HINDER"]
    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(url, params):
            eg = o.get("egenskaper", []) or []

            # prøv å plukke skiltet høyde / høyde
            e_h = pick_property(eg, ["skilt", "høyde", "fri høyde", "frihøyde"])
            hoyde = parse_float_any(e_h.get("verdi")) if e_h else None
            if hoyde is None:
                continue

            e_type = pick_property(eg, ["type", "hinder"])
            typ = str(e_type.get("verdi")).strip() if e_type and e_type.get("verdi") is not None else None

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue
            if geom.type != "point":
                geom = geom.centroid

            for s in o.get("lokasjon", {}).get("stedfestinger", []):
                if not s.get("veglenkesekvensid"):
                    continue
                cur.insertRow((
                    geom,
                    int(s["veglenkesekvensid"]),
                    float(s.get("startposisjon", 0.0)),
                    float(s.get("sluttposisjon", float(s.get("startposisjon", 0.0)))),
                    int(o["id"]),
                    hoyde,
                    typ,
                ))
                cnt += 1

    log(f"Høydebegrensning ferdig: {cnt}")
    return fc

# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    create_gdb(OUT_GDB)
    hent_vegnett(OUT_GDB)
    hent_bruer(OUT_GDB)
    hent_bruksklasse_904(OUT_GDB)
    hent_hoydebegrensning(OUT_GDB)
    log(f"✓ NVDB → GDB ferdig: {OUT_GDB}")
