# -*- coding: utf-8 -*-
"""
nvdb_to_gdb_v904.py

Robust NVDB → FileGDB for Møre og Romsdal (fylke=15), kun FV (vegsystemreferanse=F).

Henter:
  - Vegnett (segmentert)              : VEGLENKESEKV_ID + start/sluttpos + vegref/kommune
  - Bruer (objtype 60)                : alle egenskaper + filter tunnelportal/kulvert
  - Bruksklasse normaltransport (904) : BK_VERDI + MAKS_LENGDE (Spes via Merknad) + ALLE_EG
  - Høydebegrensning (objtype 591)    : skiltet høyde + alle egenskaper

ALLE_EG vises som linjeskilt liste i ArcGIS popup:
  Eier: Fylkeskommune
  Byggeår: 2016
  ...
"""

from __future__ import annotations

import os
import re
import time
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


import time

def iter_paged(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    *,
    label: str,
    log_every_page: bool = False,
    max_pages: int = 100_000,
    max_retries: int = 5,
    retry_backoff: float = 2.0,   # sekunder, dobles per forsøk
) -> Iterable[Dict[str, Any]]:
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

        # --- Retry-løkke for forbigående feil ---
        r = None
        for attempt in range(1, max_retries + 1):
            try:
                r = session.get(next_url, params=p, timeout=TIMEOUT)
                if r.status_code in (200, 404):
                    break
                # 503 / 502 / 429 → vent og prøv igjen
                wait = retry_backoff * (2 ** (attempt - 1))
                log(f"⚠️ [{label}] HTTP {r.status_code} (forsøk {attempt}/{max_retries}) — venter {wait:.0f}s...")
                time.sleep(wait)
            except requests.exceptions.ConnectionError as e:
                wait = retry_backoff * (2 ** (attempt - 1))
                log(f"⚠️ [{label}] Tilkoblingsfeil (forsøk {attempt}/{max_retries}): {e} — venter {wait:.0f}s...")
                time.sleep(wait)

        if r is None or r.status_code != 200:
            status = r.status_code if r is not None else "N/A"
            raise RuntimeError(
                f"{label}: HTTP {status} etter {max_retries} forsøk for {next_url}"
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


def eg_verdi(eg: list, *name_contains: str) -> Optional[str]:
    """Hent første match som streng (stripped)."""
    e = pick_property(eg, list(name_contains))
    if e and e.get("verdi") is not None:
        return str(e["verdi"]).strip()
    return None


def alle_eg_tekst(eg: list, max_len: int = 2000) -> Optional[str]:
    """
    Linjeskilt liste for ArcGIS popup:
      Eier: Fylkeskommune
      Byggeår: 2016
      Brukslast vegbane: Bk 10/60
      ...
    """
    if not eg:
        return None
    deler = []
    for e in eg:
        navn = e.get("navn") or f"id{e.get('id', '?')}"
        val  = e.get("verdi")
        if val is not None:
            deler.append(f"{navn}: {str(val).strip()}")
    tekst = "\n".join(deler)
    return tekst[:max_len] if len(tekst) > max_len else tekst


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
        [
            ("VEGKATEGORI", "TEXT",  1),
            ("VEGNUMMER",   "LONG"),
            ("VEGREF",      "TEXT", 50),
            ("KOMMUNE",     "TEXT", 60),
            ("FYLKE_NAVN",  "TEXT", 40),
        ],
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
    cols = [
        "SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS",
        "VEGKATEGORI", "VEGNUMMER", "VEGREF", "KOMMUNE", "FYLKE_NAVN",
    ]

    with arcpy.da.InsertCursor(fc, cols) as cur:
        for seg in iter_paged(session, url, params, label="vegnett"):
            vr = seg.get("vegsystemreferanse", {})
            if vr.get("strekning", {}).get("trafikantgruppe") != TRAFIKANTGRP:
                continue
            geom = to_geometry(seg.get("geometri"))
            if not geom:
                continue

            vs   = vr.get("vegsystem", {})
            stre = vr.get("strekning", {})
            vegref = None
            if vs.get("vegkategori") and vs.get("nummer"):
                vegref = f"{vs['vegkategori']}V{vs['nummer']}"
                if stre.get("strekning") and stre.get("delstrekning"):
                    vegref += f" S{stre['strekning']}D{stre['delstrekning']}"

            loc       = seg.get("lokasjon") or {}
            kommune   = str(loc["kommuner"][0]) if loc.get("kommuner") else None
            fylkenavn = str(loc["fylker"][0])   if loc.get("fylker")   else None

            cur.insertRow((
                geom,
                int(seg["veglenkesekvensid"]),
                float(seg.get("startposisjon", 0.0)),
                float(seg.get("sluttposisjon",  0.0)),
                vs.get("vegkategori"),
                vs.get("nummer"),
                vegref,
                kommune,
                fylkenavn,
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
        ("NVDB_ID",          "LONG"),
        ("BRU_NAVN",         "TEXT",  120),
        ("TILLATT_TONN",     "LONG"),
        ("BRUKSLAST",        "TEXT",   80),
        ("BRUTYPE",          "TEXT",   80),
        ("BRUKATEGORI",      "TEXT",   80),
        ("BYGGEAAR",         "LONG"),
        ("DRIFTSMERKING",    "TEXT",   80),
        ("EIER",             "TEXT",   80),
        ("VEDLIKEHOLDS_ANS", "TEXT",   80),
        ("LENGDE_M",         "DOUBLE"),
        ("BREDDE_M",         "DOUBLE"),
        ("TRAFIKKSTATUS",    "TEXT",   60),
        ("MERKNAD",          "TEXT",  200),
        ("ALLE_EG",          "TEXT", 2000),
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

    EKSKLUDER_BYGGVERKSTYPE = {
        "tunnelportal", "tunnel", "kulvert", "stikkrenne",
        "portal", "rørbru", "gang- og sykkelbru",
        "gang/sykkelbru", "gangbru", "vegoverbygg", "overbygg",
    }
    EKSKLUDER_BRUKATEGORI = {
        "tunnel", "vegoverbygg", "overbygg",
    }

    cnt_rows = 0
    cnt_objs = 0
    cnt_skip = 0
    err      = {"n": 0}
    cols     = [
        "SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS",
        "NVDB_ID", "BRU_NAVN", "TILLATT_TONN", "BRUKSLAST",
        "BRUTYPE", "BRUKATEGORI", "BYGGEAAR", "DRIFTSMERKING",
        "EIER", "VEDLIKEHOLDS_ANS", "LENGDE_M", "BREDDE_M",
        "TRAFIKKSTATUS", "MERKNAD", "ALLE_EG",
    ]

    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(session, url, params, label="bruer60", log_every_page=True):
            cnt_objs += 1
            if cnt_objs % 200 == 0:
                log(f"[bruer60] lest: {cnt_objs}, skrevet: {cnt_rows}, hoppet: {cnt_skip}")

            if not any(
                v.get("strekning", {}).get("trafikantgruppe") == TRAFIKANTGRP
                for v in (o.get("lokasjon") or {}).get("vegsystemreferanser", [])
            ):
                continue

            eg = o.get("egenskaper", []) or []

            navn          = eg_verdi(eg, "navn")
            brukslast     = eg_verdi(eg, "brukslast vegbane", "brukslast")
            brutype_tekst = eg_verdi(eg, "byggverkstype", "brutype", "bru type", "konstruksjonstype")
            brukategori   = eg_verdi(eg, "brukategori")
            driftsmerking = eg_verdi(eg, "driftsmerking", "brutusnummer")
            eier          = eg_verdi(eg, "eier")
            vedl_ans      = eg_verdi(eg, "vedlikeholdsansvarlig", "vedlikehold")
            merknad       = eg_verdi(eg, "merknad")

            # TRAFIKKSTATUS: eksakt felt "Status" + strip() for trailing space
            trafikkstatus = eg_verdi(eg, "status", "trafikkstatus")

            byggeaar = None
            lengde_m = None
            bredde_m = None
            tillatt  = None

            for e in eg:
                enavn = (e.get("navn") or "").lower()
                val   = e.get("verdi")

                if any(k in enavn for k in ("byggeår", "bygge år", "byggeaar")):
                    v = parse_float_any(val)
                    if v:
                        byggeaar = int(v)

                # Eksakt match "lengde" — unngår "lengste spenn", "lengde bruoverbygning" osv.
                if enavn == "lengde" and val is not None:
                    lengde_m = parse_float_any(val)

                if "bredde" in enavn and val is not None:
                    bredde_m = parse_float_any(val)

                if "brukslast" in enavn:
                    t = parse_tonn_from_text(str(val) if val is not None else None)
                    if t is not None:
                        tillatt = t

                if tillatt is None and "tillatt" in enavn and "tonn" in enavn:
                    t = parse_tonn_from_text(str(val) if val is not None else None)
                    if t is not None:
                        tillatt = t

            # Filter: tunnelportal, kulvert, gangbru osv.
            ekskluder = False
            if brutype_tekst and any(
                ekskl in brutype_tekst.lower() for ekskl in EKSKLUDER_BYGGVERKSTYPE
            ):
                ekskluder = True
            if brukategori and any(
                k in brukategori.lower() for k in EKSKLUDER_BRUKATEGORI
            ):
                ekskluder = True
            if ekskluder:
                cnt_skip += 1
                continue

            # Filter: ikke trafikkert
            if trafikkstatus and "ikke trafikkert" in trafikkstatus.lower():
                cnt_skip += 1
                continue

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue
            if geom.type == "polygon":
                geom = geom.boundary()

            alle_eg = alle_eg_tekst(eg)

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
                        brutype_tekst,
                        brukategori,
                        byggeaar,
                        driftsmerking,
                        eier,
                        vedl_ans,
                        lengde_m,
                        bredde_m,
                        trafikkstatus,
                        merknad,
                        alle_eg,
                    ),
                    err_prefix=f"[bru id={o.get('id')}] insert-feil",
                    err_counter=err,
                )
                cnt_rows += 1

    log(f"Bruer ferdig: objekter={cnt_objs}, rader={cnt_rows}, hoppet over={cnt_skip}")
    if err["n"]:
        log(f"⚠️ {err['n']} bru-rader hoppet over pga insert-feil.")
    return fc


# -------------------------
# 3. BRUKSKLASSE NORMALTRANSPORT (904)
# -------------------------
def hent_bruksklasse_904(session: requests.Session, gdb: str) -> str:
    """
    Maks vogntoglengde (10913):
      18253 = 19.5m / 18254 = 15.0m / 18255 = 12.4m → numerisk direkte
      18256 = Spesiell begrensning → ER_SPES=JA
              faktisk verdi leses fra Merknad (id 11009):
              'Maks vogntoglengde 13,30 meter.' → 13.3
    """
    log("Henter bruksklasse normaltransport (904) med posisjon...")

    fields = [
        ("NVDB_ID",         "LONG"),
        ("BK_VERDI",        "LONG"),
        ("BK_TEKST",        "TEXT",  120),
        ("MAKS_LENGDE",     "DOUBLE"),
        ("ER_SPES",         "TEXT",    5),
        ("STREKNINGSBESKR", "TEXT",  200),
        ("VEGLISTE_INFO",   "TEXT",  120),
        ("MERKNAD",         "TEXT",  200),
        ("GYLDIG_FRA",      "TEXT",   20),
        ("GYLDIG_TIL",      "TEXT",   20),
        ("ALLE_EG",         "TEXT", 2000),
    ]
    fc = create_fc(gdb, "Bruksklasse_904", "POLYLINE", fields)

    url    = f"{VEGOBJ_API}/vegobjekter/{OBJ_BK}"
    params = {
        "fylke":              FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall":             1000,
        "inkluder":           "egenskaper,lokasjon,geometri,metadata",
        "srid":               SRID,
        "alle_versjoner":     "false",
    }

    cnt      = 0
    spes_cnt = 0
    err      = {"n": 0}
    cols     = [
        "SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS",
        "NVDB_ID", "BK_VERDI", "BK_TEKST", "MAKS_LENGDE", "ER_SPES",
        "STREKNINGSBESKR", "VEGLISTE_INFO", "MERKNAD",
        "GYLDIG_FRA", "GYLDIG_TIL", "ALLE_EG",
    ]

    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(session, url, params, label="bk904"):
            eg = o.get("egenskaper", []) or []

            bk_text         = None
            bk_val          = None
            maks_len        = None
            er_spes         = "NEI"
            spes_len        = None
            merknad_tekst   = eg_verdi(eg, "merknad")
            strekningsbeskr = eg_verdi(eg, "strekningsbeskrivelse")
            vegliste_info   = eg_verdi(eg, "vegliste")

            meta       = o.get("metadata") or {}
            gyldig_fra = str(meta.get("startdato") or "") or None
            gyldig_til = str(meta.get("sluttdato") or "") or None

            for e in eg:
                enavn = (e.get("navn") or "").lower()
                val   = e.get("verdi")

                if any(k in enavn for k in ("bruksklasse", "helår", "vinter")):
                    if bk_text is None and val is not None:
                        bk_text = str(val).strip()
                        bk_val  = parse_tonn_from_text(bk_text)

                if (
                    "vogntoglengde" in enavn
                    and "skiltet"   not in enavn
                    and "modul"     not in enavn
                    and "tømmer"    not in enavn
                    and val is not None
                ):
                    parsed = parse_float_any(val)
                    if parsed is not None:
                        if maks_len is None:
                            maks_len = parsed
                    else:
                        if "spes" in str(val).lower():
                            er_spes = "JA"

                if (
                    "skiltet" in enavn
                    and (
                        "vogntoglengde"     in enavn
                        or "kjøretøylengde" in enavn
                        or "lengde"         in enavn
                    )
                    and val is not None
                ):
                    parsed = parse_float_any(val)
                    if parsed is not None:
                        spes_len = parsed

            # Spes: skiltet-felt → Merknad som fallback
            if er_spes == "JA":
                if spes_len is not None:
                    maks_len = spes_len
                elif merknad_tekst is not None:
                    parsed = parse_float_any(merknad_tekst)
                    if parsed is not None:
                        maks_len = parsed

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue

            if er_spes == "JA":
                spes_cnt += 1

            alle_eg = alle_eg_tekst(eg)

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
                        bk_val,
                        bk_text,
                        maks_len,
                        er_spes,
                        strekningsbeskr,
                        vegliste_info,
                        merknad_tekst,
                        gyldig_fra,
                        gyldig_til,
                        alle_eg,
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
            log(f"  ⚠️  {spes_null} Spes-objekt(er) mangler lengdeverdi — sjekk MERKNAD-feltet.")
        else:
            log(f"  ✅ Alle {spes_cnt} Spes-objekter har lengdeverdi.")
    if err["n"]:
        log(f"⚠️ {err['n']} BK 904-rader hoppet over pga insert-feil.")
    return fc


# -------------------------
# 4. HØYDEBEGRENSNING (591)
# -------------------------
def hent_hoydebegrensning(session: requests.Session, gdb: str) -> str:
    log("Henter høydebegrensning (591) med posisjon...")

    fields = [
        ("NVDB_ID",        "LONG"),
        ("SKILTET_HOYDE",  "DOUBLE"),
        ("TYPE_HINDER",    "TEXT",   60),
        ("HINDER_NAVN",    "TEXT",  120),
        ("EIER",           "TEXT",   80),
        ("MERKNAD",        "TEXT",  200),
        ("GYLDIG_FRA",     "TEXT",   20),
        ("GYLDIG_TIL",     "TEXT",   20),
        ("ALLE_EG",        "TEXT", 2000),
    ]
    fc = create_fc(gdb, "Hoydebegrensning_591", "POINT", fields)

    url    = f"{VEGOBJ_API}/vegobjekter/{OBJ_HOY}"
    params = {
        "fylke":              FYLKE,
        "vegsystemreferanse": VEGSYSTEMREF,
        "antall":             1000,
        "inkluder":           "egenskaper,lokasjon,geometri,metadata",
        "srid":               SRID,
        "alle_versjoner":     "false",
    }

    cnt  = 0
    cols = [
        "SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS",
        "NVDB_ID", "SKILTET_HOYDE", "TYPE_HINDER", "HINDER_NAVN",
        "EIER", "MERKNAD", "GYLDIG_FRA", "GYLDIG_TIL", "ALLE_EG",
    ]

    with arcpy.da.InsertCursor(fc, cols) as cur:
        for o in iter_paged(session, url, params, label="hoyde591"):
            eg    = o.get("egenskaper", []) or []
            e_h   = pick_property(eg, ["skiltet høyde", "fri høyde", "frihøyde", "høyde"])
            hoyde = parse_float_any(e_h.get("verdi")) if e_h else None

            if hoyde is None:
                continue

            typ         = eg_verdi(eg, "type", "hinder")
            hinder_navn = eg_verdi(eg, "navn")
            eier        = eg_verdi(eg, "eier")
            merknad     = eg_verdi(eg, "merknad")

            meta       = o.get("metadata") or {}
            gyldig_fra = str(meta.get("startdato") or "") or None
            gyldig_til = str(meta.get("sluttdato") or "") or None

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue
            if geom.type != "point":
                geom = geom.centroid

            alle_eg = alle_eg_tekst(eg)

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
                    hinder_navn,
                    eier,
                    merknad,
                    gyldig_fra,
                    gyldig_til,
                    alle_eg,
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
