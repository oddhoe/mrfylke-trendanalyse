# -*- coding: utf-8 -*-
"""
nvdb_to_gdb_v904.py

Robust NVDB → FileGDB for Møre og Romsdal (fylke=15), kun FV (vegsystemreferanse=F).

Henter:
  - Vegnett (segmentert)              : VEGLENKESEKV_ID + start/sluttpos
  - Bruer (objtype 60)                : posisjon + tillatt tonn (pars fra tekst)
  - Bruksklasse normaltransport (904) : posisjon + BK_VERDI + MAKS_LENGDE + ER_SPES
  - Høydebegrensning (objtype 591)    : posisjon + skiltet høyde

Endringer:
  - hent_bruksklasse_904: Spes (18256) → leser Merknad (id 11009) som fritekst-fallback.
    Trollstigen: 'Maks vogntoglengde 13,30 meter.' → MAKS_LENGDE = 13.3
  - ER_SPES = "JA"/"NEI" for sporbarhet i nedstrøms-steg.
  - Paginering: bruker metadata.neste.start, fallback href, sikkerhetsbryter.
  - Requests Session + timeouts.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Iterable, Optional

import arcpy
import requests

arcpy.env.overwriteOutput = True

# -------------------------
# KONFIG
# -------------------------
FYLKE        = 15
SRID         = 5973
NVDB_API     = "https://nvdbapiles.atlas.vegvesen.no"
VEGNETT_API  = f"{NVDB_API}/vegnett/api/v4"
VEGOBJ_API   = f"{NVDB_API}/vegobjekter/api/v4"
OUT_GDB      = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"

VEGSYSTEMREF = "F"   # FV
TRAFIKANTGRP = "K"   # Kjørefelt

OBJ_BRU = 60
OBJ_BK  = 904
OBJ_HOY = 591

HEADERS = {
    "X-Client": "mrfk_flaskehalsanalyse",
    "Accept":   "application/vnd.vegvesen.nvdb-v3+json",
}
TIMEOUT = 60


# -------------------------
# HJELPEFUNKSJONER
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
    max_pages: int = 100_000,
) -> Iterable[Dict[str, Any]]:
    """
    Generator for paginert NVDB v4.
    Foretrekker metadata.neste.start, faller tilbake til href.
    Har sikring mot repeterende start/href.
    """
    start: Optional[str] = None
    seen_starts: set[str] = set()
    seen_hrefs:  set[str] = set()
    page     = 0
    next_url = url

    while True:
        page += 1
        if page > max_pages:
            raise RuntimeError(
                f"{label}: Stoppet etter {max_pages} sider (sikkerhetsbryter)."
            )

        p = dict(params)
        if start:
            p["start"] = start

        if log_every_page:
            log(f"[{label}] side {page} start={start!r}")

        r = session.get(next_url, params=p, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(
                f"{label}: HTTP {r.status_code} for {r.url}: {r.text[:200]}"
            )

        data = r.json()
        objs = data.get("objekter", []) or []
        if not objs:
            return

        yield from objs

        nxt       = (data.get("metadata") or {}).get("neste") or {}
        nxt_start = nxt.get("start")

        if nxt_start:
            if nxt_start in seen_starts:
                log(f"⚠️ {label}: neste.start repeteres ({nxt_start!r}). Avbryter.")
                return
            seen_starts.add(nxt_start)
            start = str(nxt_start)
            continue

        href = nxt.get("href")
        if href:
            if href in seen_hrefs:
                log(f"⚠️ {label}: neste.href repeteres. Avbryter.")
                return
            seen_hrefs.add(href)
            next_url = href
            params   = {}
            start    = None
            continue

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
    arcpy.management.AddField(fc, "STARTPOS",        "DOUBLE")
    arcpy.management.AddField(fc, "SLUTTPOS",        "DOUBLE")
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
    """Trekk ut tonnverdi fra tekst (BK10/60, 12/65, '60 tonn' o.l.)."""
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


def pick_property(
    egenskaper: list[Dict[str, Any]],
    name_contains: list[str],
) -> Optional[Dict[str, Any]]:
    if not egenskaper:
        return None
    for e in egenskaper:
        navn = (e.get("navn") or "").lower()
        if any(sub.lower() in navn for sub in name_contains):
            return e
    return None


def safe_insert(
    cur,
    row,
    *,
    err_prefix: str,
    err_counter: dict,
    max_print: int = 10,
) -> None:
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

    fc = create_fc(
        gdb, "Vegnett", "POLYLINE",
        [("VEGKATEGORI", "TEXT", 1), ("VEGNUMMER", "LONG")],
    )

    url    = f"{VEGNETT_API}/veglenkesekvenser/segmentert"
    params = {
        "fylke":              FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall":             5000,
        "inkluderAntall":     "false",
        "srid":               SRID,
    }

    cnt  = 0
    cols = ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "VEGKATEGORI", "VEGNUMMER"]

    with arcpy.da.InsertCursor(fc, cols) as cur:
        for seg in iter_paged(session, url, params, label="vegnett"):
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
                float(seg.get("sluttposisjon",  0.0)),
                vr.get("vegsystem", {}).get("vegkategori"),
                vr.get("vegsystem", {}).get("nummer"),
            ))
            cnt += 1

    log(f"Vegnett ferdig: {cnt} segmenter")
    return fc


# -------------------------
# 2. BRUER (60)
# -------------------------
def hent_bruer(session: requests.Session, gdb: str) -> str:
    log("Henter bruer (60) med posisjon...")

    fields = [
        ("BRU_ID",       "LONG"),
        ("BRU_NAVN",     "TEXT", 120),
        ("TILLATT_TONN", "LONG"),
        ("BRUKSLAST",    "TEXT", 80),
    ]
    fc = create_fc(gdb, "Bruer", "POLYLINE", fields)

    url    = f"{VEGOBJ_API}/vegobjekter/{OBJ_BRU}"
    params = {
        "fylke":              FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall":             1000,
        "inkluder":           "egenskaper,lokasjon,geometri",
        "srid":               SRID,
        "alle_versjoner":     "false",
    }

    cnt_rows = 0
    cnt_objs = 0
    err      = {"n": 0}
    cols     = [
        "SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS",
        "BRU_ID", "BRU_NAVN", "TILLATT_TONN", "BRUKSLAST",
    ]

    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(session, url, params, label="bruer60", log_every_page=True):
            cnt_objs += 1
            if cnt_objs % 200 == 0:
                log(f"[bruer60] objekter lest: {cnt_objs}, rader skrevet: {cnt_rows}")

            if not any(
                v.get("strekning", {}).get("trafikantgruppe") == TRAFIKANTGRP
                for v in (o.get("lokasjon") or {}).get("vegsystemreferanser", [])
            ):
                continue

            navn          = None
            brukslast     = None
            tillatt       = None
            er_vegbru     = None
            er_trafikkert = None

            for e in o.get("egenskaper", []) or []:
                enavn = (e.get("navn") or "").lower()
                val   = e.get("verdi")

                if "navn" in enavn and navn is None:
                    navn = str(val).strip() if val is not None else None

                if "bru type" in enavn or enavn.endswith("brutype"):
                    er_vegbru = (
                        str(val).strip().lower() == "vegbru"
                        if val is not None else er_vegbru
                    )

                if "trafikk" in enavn and "status" in enavn:
                    er_trafikkert = (
                        str(val).strip().lower() == "trafikkert"
                        if val is not None else er_trafikkert
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
                        float(s.get("sluttposisjon",  0.0)),
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
        log(f"⚠️ {err['n']} bru-rader hoppet over pga insert-feil.")
    return fc


# -------------------------
# 3. BRUKSKLASSE NORMALTRANSPORT (904)
# -------------------------
def hent_bruksklasse_904(session: requests.Session, gdb: str) -> str:
    """
    Henter BK 904 fra NVDB.

    Maks vogntoglengde (egenskapstype 10913):
      18253 = 19,50 m  →  parse_float_any gir 19.5
      18254 = 15,00 m  →  parse_float_any gir 15.0
      18255 = 12,40 m  →  parse_float_any gir 12.4
      18256 = Spesiell begrensning ("Spes")
              →  ER_SPES = "JA"
              →  faktisk verdi leses fra Merknad (id 11009) som fritekst:
                 "Maks vogntoglengde 13,30 meter." → 13.3
                 "13,10 meter"                     → 13.1
    """
    log("Henter bruksklasse normaltransport (904) med posisjon...")

    fields = [
        ("BK_VERDI",    "LONG"),
        ("BK_TEKST",    "TEXT", 120),
        ("MAKS_LENGDE", "DOUBLE"),
        ("ER_SPES",     "TEXT", 5),    # "JA" / "NEI"
        ("MERKNAD",     "TEXT", 200),  # rå merknad-tekst for sporbarhet
    ]
    fc = create_fc(gdb, "Bruksklasse_904", "POLYLINE", fields)

    url    = f"{VEGOBJ_API}/vegobjekter/{OBJ_BK}"
    params = {
        "fylke":              FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall":             1000,
        "inkluder":           "egenskaper,lokasjon,geometri",
        "srid":               SRID,
        "alle_versjoner":     "false",
    }

    cnt      = 0
    spes_cnt = 0
    err      = {"n": 0}
    cols     = [
        "SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS",
        "BK_VERDI", "BK_TEKST", "MAKS_LENGDE", "ER_SPES", "MERKNAD",
    ]

    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(session, url, params, label="bk904"):
            eg = o.get("egenskaper", []) or []

            bk_text       = None
            bk_val        = None
            maks_len      = None
            er_spes       = "NEI"
            spes_len      = None   # fra dedikert "skiltet"-felt (fremtidig støtte)
            merknad_tekst = None   # fra Merknad (id 11009) — brukes for Spes

            for e in eg:
                enavn = (e.get("navn") or "").lower()
                val   = e.get("verdi")

                # --- Bruksklasse-tekst ---
                if any(k in enavn for k in ("bruksklasse", "helår", "vinter")):
                    if bk_text is None and val is not None:
                        bk_text = str(val).strip()
                        bk_val  = parse_tonn_from_text(bk_text)

                # --- Maks vogntoglengde (hoved-enumfelt) ---
                if (
                    "vogntoglengde" in enavn
                    and "skiltet"   not in enavn
                    and "modul"     not in enavn
                    and "tømmer"    not in enavn
                    and val is not None
                ):
                    parsed = parse_float_any(val)
                    if parsed is not None:
                        # Direkte numerisk verdi (19.5 / 15.0 / 12.4)
                        if maks_len is None:
                            maks_len = parsed
                    else:
                        # Ikke-numerisk → sjekk for Spes
                        if "spes" in str(val).lower():
                            er_spes = "JA"

                # --- Maks vogntoglengde SKILTET (dedikert felt, om det finnes) ---
                if (
                    "skiltet" in enavn
                    and (
                        "vogntoglengde"    in enavn
                        or "kjøretøylengde" in enavn
                        or "lengde"         in enavn
                    )
                    and val is not None
                ):
                    parsed = parse_float_any(val)
                    if parsed is not None:
                        spes_len = parsed

                # --- Merknad (id 11009) — fritekst med faktisk Spes-lengde ---
                if "merknad" in enavn and val is not None:
                    merknad_tekst = str(val).strip()

            # --- Bestem endelig MAKS_LENGDE for Spes-objekter ---
            if er_spes == "JA":
                if spes_len is not None:
                    # Dedikert skiltet-felt (høyeste prioritet)
                    maks_len = spes_len
                elif merknad_tekst is not None:
                    # Fritekst-fallback: "Maks vogntoglengde 13,30 meter." → 13.3
                    parsed = parse_float_any(merknad_tekst)
                    if parsed is not None:
                        maks_len = parsed

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue

            if er_spes == "JA":
                spes_cnt += 1

            for s in (o.get("lokasjon") or {}).get("stedfestinger", []) or []:
                if not s.get("veglenkesekvensid"):
                    continue
                safe_insert(
                    cur,
                    (
                        geom,
                        int(s["veglenkesekvensid"]),
                        float(s.get("startposisjon", 0.0)),
                        float(s.get("sluttposisjon",  0.0)),
                        bk_val,
                        bk_text,
                        maks_len,
                        er_spes,
                        merknad_tekst,
                    ),
                    err_prefix=f"[bk904 id={o.get('id')}] insert-feil",
                    err_counter=err,
                )
                cnt += 1

    log(f"Bruksklasse 904 ferdig: {cnt} rader  (herav Spes-objekter: {spes_cnt})")

    if spes_cnt:
        spes_null = sum(
            1 for r in arcpy.da.SearchCursor(fc, ["ER_SPES", "MAKS_LENGDE"])
            if r[0] == "JA" and r[1] is None
        )
        if spes_null:
            log(
                f"  ⚠️  {spes_null} Spes-objekt(er) mangler lengdeverdi "
                f"(hverken skiltet-felt eller Merknad). Sjekk i vegkart."
            )
        else:
            log(f"  ✅ Alle {spes_cnt} Spes-objekter har lengdeverdi fra Merknad/skiltet.")

    if err["n"]:
        log(f"⚠️ {err['n']} BK 904-rader hoppet over pga insert-feil.")

    return fc


# -------------------------
# 4. HØYDEBEGRENSNING (591)
# -------------------------
def hent_hoydebegrensning(session: requests.Session, gdb: str) -> str:
    log("Henter høydebegrensning (591) med posisjon...")

    fields = [
        ("NVDB_ID",       "LONG"),
        ("SKILTET_HOYDE", "DOUBLE"),
        ("TYPE_HINDER",   "TEXT", 60),
    ]
    fc = create_fc(gdb, "Hoydebegrensning_591", "POINT", fields)

    url    = f"{VEGOBJ_API}/vegobjekter/{OBJ_HOY}"
    params = {
        "fylke":              FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall":             1000,
        "inkluder":           "egenskaper,lokasjon,geometri",
        "srid":               SRID,
        "alle_versjoner":     "false",
    }

    cnt  = 0
    cols = [
        "SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS",
        "NVDB_ID", "SKILTET_HOYDE", "TYPE_HINDER",
    ]

    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(session, url, params, label="hoyde591"):
            eg    = o.get("egenskaper", []) or []
            e_h   = pick_property(eg, ["skilt", "høyde", "fri høyde", "frihøyde"])
            hoyde = parse_float_any(e_h.get("verdi")) if e_h else None

            if hoyde is None:
                continue

            e_type = pick_property(eg, ["type", "hinder"])
            typ    = (
                str(e_type.get("verdi")).strip()
                if (e_type and e_type.get("verdi") is not None)
                else None
            )

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
                cur.insertRow((
                    geom,
                    int(s["veglenkesekvensid"]),
                    startpos,
                    sluttpos,
                    int(o["id"]),
                    hoyde,
                    typ,
                ))
                cnt += 1

    log(f"Høydebegrensning ferdig: {cnt} punkter")
    return fc


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    session = create_session()
    create_gdb(OUT_GDB)

    log("=" * 60)
    hent_vegnett(session, OUT_GDB)

    log("=" * 60)
    hent_bruer(session, OUT_GDB)

    log("=" * 60)
    hent_bruksklasse_904(session, OUT_GDB)

    log("=" * 60)
    hent_hoydebegrensning(session, OUT_GDB)

    log("=" * 60)
    log(f"✅ NVDB → GDB ferdig: {OUT_GDB}")
