# -*- coding: utf-8 -*-
"""nvdb_to_gdb_v904_v2.py

Robust NVDB → FileGDB for Møre og Romsdal (fylke=15), kun FV (vegsystemreferanse=F).
Henter:
- Vegnett (segmentert) med VEGLENKESEKV_ID + start/sluttpos
- Bruer (objtype 60) med posisjon + tillatt tonn (pars fra tekst)
- Bruksklasse normaltransport (objtype 904) med posisjon + (BK_VERDI tonn, MAKS_LENGDE)
- Høydebegrensning (objtype 591) med posisjon + skiltet høyde

Endringer vs forrige:
- Paginering: bruker primært metadata.neste.start (unngår evig loop på href-repetisjon)
- Sikkerhetsbryter mot repeterende start/href
- Progress-logging per side for bruer (så det ikke ser "frosset" ut)
- Requests Session + timeouts
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Iterable, Optional, Tuple

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
VEGOBJ_API = f"{NVDB_API}/vegobjekter/api/v4"

OUT_GDB = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"

VEGSYSTEMREF = "F"  # FV
TRAFIKANTGRP = "K"  # Kjørefelt

OBJ_BRU = 60
OBJ_BK = 904
OBJ_HOY = 591

HEADERS = {
    "X-Client": "mrfk_flaskehalsanalyse",
    "Accept": "application/vnd.vegvesen.nvdb-v3+json",
}

TIMEOUT = 60

# -------------------------
# HJELP
# -------------------------

def log(msg: str) -> None:
    print(msg)


def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def iter_paged(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    *,
    label: str,
    log_every_page: bool = False,
    max_pages: int = 100000,
) -> Iterable[Dict[str, Any]]:
    """Generator for paginert NVDB v4.

    Foretrekker metadata.neste.start (stabilt), faller tilbake til metadata.neste.href.
    Har sikring mot repeterende start/href.
    """
    start: Optional[str] = None
    seen_starts: set[str] = set()
    seen_hrefs: set[str] = set()

    page = 0
    next_url = url

    while True:
        page += 1
        if page > max_pages:
            raise RuntimeError(f"{label}: Stoppet etter {max_pages} sider (sikkerhetsbryter).")

        p = dict(params)
        if start:
            p["start"] = start

        if log_every_page:
            log(f"[{label}] side {page} start={start!r}")

        r = session.get(next_url, params=p, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"{label}: HTTP {r.status_code} for {r.url}: {r.text[:200]}")

        data = r.json()
        objs = data.get("objekter", []) or []
        if not objs:
            return

        for o in objs:
            yield o

        nxt = (data.get("metadata") or {}).get("neste") or {}

        # 1) Preferer start-token
        nxt_start = nxt.get("start")
        if nxt_start:
            if nxt_start in seen_starts:
                log(f"⚠️ {label}: neste.start repeteres ({nxt_start!r}). Avbryter paginering.")
                return
            seen_starts.add(nxt_start)
            start = str(nxt_start)
            continue

        # 2) Fallback: href
        href = nxt.get("href")
        if href:
            if href in seen_hrefs:
                log(f"⚠️ {label}: neste.href repeteres. Avbryter paginering.")
                return
            seen_hrefs.add(href)
            # Når vi bruker href, kjører vi uten params videre
            next_url = href
            params = {}
            start = None
            continue

        # 3) Ingen neste
        return


def to_geometry(geom: Optional[Dict[str, Any]]):
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
        if len(f) == 2:
            arcpy.management.AddField(fc, f[0], f[1])
        else:
            arcpy.management.AddField(fc, f[0], f[1], field_length=f[2])

    return fc


_num_re = re.compile(r"(\d+(?:[.,]\d+)?)")


def parse_float_any(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    m = _num_re.search(s)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def parse_tonn_from_text(s: Optional[str]) -> Optional[int]:
    """Trekk ut tonnverdi fra tekst (typisk BK10/60, 12/65, "60 tonn")."""
    if not s:
        return None
    m = re.search(r"/\s*(\d+)", s)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*tonn", s, re.IGNORECASE)
    if m:
        return int(m.group(1))
    nums = [int(n) for n in re.findall(r"(\d+)", s)]
    return max(nums) if nums else None


def pick_property(egenskaper: list[Dict[str, Any]], name_contains: list[str]) -> Optional[Dict[str, Any]]:
    if not egenskaper:
        return None
    for e in egenskaper:
        navn = (e.get("navn") or "").lower()
        for sub in name_contains:
            if sub.lower() in navn:
                return e
    return None


def safe_insert(cur, row, *, err_prefix: str, err_counter: dict, max_print: int = 10) -> None:
    try:
        cur.insertRow(row)
    except Exception as e:
        err_counter["n"] += 1
        if err_counter["n"] <= max_print:
            log(f"{err_prefix}: {e}")


# -------------------------
# 1. VEGNETT
# -------------------------

def hent_vegnett(session: requests.Session, gdb: str) -> str:
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
        for seg in iter_paged(session, url, params, label="vegnett"):
            vr = seg.get("vegsystemreferanse", {})
            if vr.get("strekning", {}).get("trafikantgruppe") != TRAFIKANTGRP:
                continue
            geom = to_geometry(seg.get("geometri"))
            if not geom:
                continue

            cur.insertRow(
                (
                    geom,
                    int(seg["veglenkesekvensid"]),
                    float(seg.get("startposisjon", 0.0)),
                    float(seg.get("sluttposisjon", 0.0)),
                    vr.get("vegsystem", {}).get("vegkategori"),
                    vr.get("vegsystem", {}).get("nummer"),
                )
            )
            cnt += 1

    log(f"Vegnett ferdig: {cnt}")
    return fc


# -------------------------
# 2. BRUER (60)
# -------------------------

def hent_bruer(session: requests.Session, gdb: str) -> str:
    log("Henter bruer (60) med posisjon...")
    fields = [("BRU_ID", "LONG"), ("BRU_NAVN", "TEXT", 120), ("TILLATT_TONN", "LONG"), ("BRUKSLAST", "TEXT", 80)]
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

    cnt_rows = 0
    cnt_objs = 0
    err = {"n": 0}

    cols = [
        "SHAPE@",
        "VEGLENKESEKV_ID",
        "STARTPOS",
        "SLUTTPOS",
        "BRU_ID",
        "BRU_NAVN",
        "TILLATT_TONN",
        "BRUKSLAST",
    ]

    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(session, url, params, label="bruer60", log_every_page=True):
            cnt_objs += 1
            if cnt_objs % 200 == 0:
                log(f"[bruer60] objekter lest: {cnt_objs}, rader skrevet: {cnt_rows}")

            # Trafikantgruppe-filter
            if not any(
                v.get("strekning", {}).get("trafikantgruppe") == TRAFIKANTGRP
                for v in (o.get("lokasjon") or {}).get("vegsystemreferanser", [])
            ):
                continue

            navn = None
            brukslast = None
            tillatt = None
            er_vegbru = None
            er_trafikkert = None

            for e in o.get("egenskaper", []) or []:
                enavn = (e.get("navn") or "").lower()
                val = e.get("verdi")

                if "navn" in enavn and navn is None:
                    navn = str(val).strip() if val is not None else None

                if "bru type" in enavn or enavn.endswith("brutype"):
                    er_vegbru = (str(val).strip().lower() == "vegbru") if val is not None else er_vegbru

                if "trafikk" in enavn and "status" in enavn:
                    er_trafikkert = (
                        str(val).strip().lower() == "trafikkert" if val is not None else er_trafikkert
                    )

                if "brukslast" in enavn:
                    brukslast = str(val).strip() if val is not None else None
                    t = parse_tonn_from_text(brukslast)
                    if t is not None:
                        tillatt = t

                if tillatt is None and ("tillatt" in enavn and "tonn" in enavn):
                    t = parse_tonn_from_text(str(val).strip() if val is not None else None)
                    if t is not None:
                        tillatt = t

            if er_vegbru is False:
                continue
            if er_trafikkert is False:
                continue

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue
            if geom.type == "polygon":
                geom = geom.boundary()

            for s in (o.get("lokasjon") or {}).get("stedfestinger", []) or []:
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
                    err_prefix=f"[bru id={o.get('id')} vls={s.get('veglenkesekvensid')}] insert-feil",
                    err_counter=err,
                )
                cnt_rows += 1

    log(f"Bruer ferdig: objekter={cnt_objs}, rader={cnt_rows}")
    if err["n"]:
        log(f"⚠️ Advarsel: {err['n']} bru-rader ble hoppet over pga insert-feil.")
    return fc


# -------------------------
# 3. BRUKSKLASSE NORMALTRANSPORT (904)
# -------------------------

def hent_bruksklasse_904(session: requests.Session, gdb: str) -> str:
    log("Henter bruksklasse normaltransport (904) med posisjon...")
    fields = [("BK_VERDI", "LONG"), ("BK_TEKST", "TEXT", 120), ("MAKS_LENGDE", "DOUBLE")]
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
        for o in iter_paged(session, url, params, label="bk904"):
            eg = o.get("egenskaper", []) or []

            e_bk = pick_property(eg, ["bruksklasse", "bk", "helår", "vinter"])
            e_len = pick_property(eg, ["vogntoglengde", "lengde"])

            bk_text = str(e_bk.get("verdi")).strip() if (e_bk and e_bk.get("verdi") is not None) else None
            bk_val = parse_tonn_from_text(bk_text)

            maks_len = parse_float_any(e_len.get("verdi")) if (e_len and e_len.get("verdi") is not None) else None

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue

            for s in (o.get("lokasjon") or {}).get("stedfestinger", []) or []:
                if not s.get("veglenkesekvensid"):
                    continue
                cur.insertRow(
                    (
                        geom,
                        int(s["veglenkesekvensid"]),
                        float(s.get("startposisjon", 0.0)),
                        float(s.get("sluttposisjon", 0.0)),
                        bk_val,
                        bk_text,
                        maks_len,
                    )
                )
                cnt += 1

    log(f"Bruksklasse 904 ferdig: {cnt}")
    return fc


# -------------------------
# 4. HØYDEBEGRENSNING (591)
# -------------------------

def hent_hoydebegrensning(session: requests.Session, gdb: str) -> str:
    log("Henter høydebegrensning (591) med posisjon...")
    fields = [("NVDB_ID", "LONG"), ("SKILTET_HOYDE", "DOUBLE"), ("TYPE_HINDER", "TEXT", 60)]
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
        for o in iter_paged(session, url, params, label="hoyde591"):
            eg = o.get("egenskaper", []) or []

            e_h = pick_property(eg, ["skilt", "høyde", "fri høyde", "frihøyde"])
            hoyde = parse_float_any(e_h.get("verdi")) if e_h else None
            if hoyde is None:
                continue

            e_type = pick_property(eg, ["type", "hinder"])
            typ = str(e_type.get("verdi")).strip() if (e_type and e_type.get("verdi") is not None) else None

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue
            if geom.type != "point":
                geom = geom.centroid

            for s in (o.get("lokasjon") or {}).get("stedfestinger", []) or []:
                if not s.get("veglenkesekvensid"):
                    continue
                startpos = float(s.get("startposisjon", 0.0))
                sluttpos = float(s.get("sluttposisjon", startpos))
                cur.insertRow((geom, int(s["veglenkesekvensid"]), startpos, sluttpos, int(o["id"]), hoyde, typ))
                cnt += 1

    log(f"Høydebegrensning ferdig: {cnt}")
    return fc


# -------------------------
# MAIN
# -------------------------

if __name__ == "__main__":
    session = create_session()
    create_gdb(OUT_GDB)
    hent_vegnett(session, OUT_GDB)
    hent_bruer(session, OUT_GDB)
    hent_bruksklasse_904(session, OUT_GDB)
    hent_hoydebegrensning(session, OUT_GDB)
    log(f"✓ NVDB → GDB ferdig: {OUT_GDB}")
