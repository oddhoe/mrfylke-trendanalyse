#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kjørelogg 2026 – FV vegnett (K og G) + registreringsnett (propagering/dissolve)
ROBUST og deterministisk init av Status_Måling = "IKKE MÅLT" for ALLE rader.

Bygger 2 nivå:
1) Detaljvegnett (segmentert fra NVDB):  Vegnett_FV_K  / Vegnett_FV_G
2) Registreringsnett (propagert):       Regnett_FV_K   / Regnett_FV_G
   - Dissolve på (TRAFIKANTGRP, VEGKATEGORI, VEGNUMMER, Driftskontrakt)
   - Median av segmentlengde i gruppa legges på som Lengde_km_median

Inkluderer:
- MR (fylke=15) FV
- Vestland FV61 i Stad kommune 4649 (Bryggja-området)

Krever: ArcGIS Pro Python (arcpy), requests
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, Optional

import arcpy
import requests

arcpy.env.overwriteOutput = True

# -------------------------
# KONFIG
# -------------------------
NVDB_API = "https://nvdbapiles.atlas.vegvesen.no"
VEGNETT_API = f"{NVDB_API}/vegnett/api/v4"

OUT_FOLDER = r"G:\Test\2026\Output"
OUT_GDB = os.path.join(OUT_FOLDER, "Kjorelogg_2026.gdb")

# Driftskontrakt (fra din gpkg)
DRIFTSKONTRAKT_GPKG = r"G:\Test\2026\Ansvarsområder.gpkg"
DRIFTSKONTRAKT_LAYER = "main.Driftskontraksområder"
KONTRAKT_FELT = "Kontraksområde"

# NVDB-geometri
SRID = 5973
SR = arcpy.SpatialReference(SRID)

# MR
MR_FYLKE = 15
VEGSYSTEMREF_MR = "F"  # fungerer i ditt v904-skript

# Vestland tillegg: FV61 i Stad kommune 4649
VL_FYLKE = 46
VL_KOMM = 4649
VEGSYSTEMREF_VL = "Fv61"

HEADERS = {
    "X-Client": "mrfk_kjorelogg_2026",
    "Accept": "application/vnd.vegvesen.nvdb-v3+json",
}
TIMEOUT = 60


# -------------------------
# LOGG
# -------------------------
def log(msg: str) -> None:
    print(msg)


# -------------------------
# HTTP + paging
# -------------------------
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
    max_pages: int = 200_000,
    max_retries: int = 5,
    retry_backoff: float = 2.0,
) -> Iterable[Dict[str, Any]]:
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

        r = None
        for attempt in range(1, max_retries + 1):
            try:
                r = session.get(next_url, params=p, timeout=TIMEOUT)
                if r.status_code == 200:
                    break
                wait = retry_backoff * (2 ** (attempt - 1))
                log(f"⚠️ [{label}] HTTP {r.status_code} (forsøk {attempt}/{max_retries}) — venter {wait:.0f}s...")
                time.sleep(wait)
            except requests.exceptions.ConnectionError as e:
                wait = retry_backoff * (2 ** (attempt - 1))
                log(f"⚠️ [{label}] Tilkoblingsfeil (forsøk {attempt}/{max_retries}): {e} — venter {wait:.0f}s...")
                time.sleep(wait)

        if r is None or r.status_code != 200:
            status = r.status_code if r is not None else "N/A"
            txt = r.text if r is not None else ""
            raise RuntimeError(f"{label}: HTTP {status} etter {max_retries} forsøk. Svar: {txt[:800]}")

        data = r.json()
        objs = data.get("objekter", []) or []
        if not objs:
            return

        yield from objs

        nxt = (data.get("metadata") or {}).get("neste") or {}
        nxt_start = nxt.get("start")
        if nxt_start is not None:
            nxt_start = str(nxt_start)
            if nxt_start in seen_starts:
                log(f"⚠️ {label}: neste.start repeteres ({nxt_start!r}). Avbryter.")
                return
            seen_starts.add(nxt_start)
            start = nxt_start
            continue

        href = nxt.get("href")
        if href:
            if href in seen_hrefs:
                log(f"⚠️ {label}: neste.href repeteres. Avbryter.")
                return
            seen_hrefs.add(href)
            next_url = href
            params = {}
            start = None
            continue

        return


# -------------------------
# Geometri
# -------------------------
def to_geometry(geom: Optional[Dict[str, Any]]):
    if not geom:
        return None
    wkt = geom.get("wkt")
    if not wkt:
        return None
    try:
        return arcpy.FromWKT(wkt, SR)
    except Exception:
        return None


# -------------------------
# GDB/FC
# -------------------------
def create_gdb(path: str) -> None:
    folder, name = os.path.split(path)
    os.makedirs(folder, exist_ok=True)
    if arcpy.Exists(path):
        arcpy.management.Delete(path)
    arcpy.management.CreateFileGDB(folder, name)


def create_fc(gdb: str, name: str, geom_type: str, extra_fields: list[tuple]) -> str:
    fc = os.path.join(gdb, name)
    if arcpy.Exists(fc):
        arcpy.management.Delete(fc)

    arcpy.management.CreateFeatureclass(gdb, name, geom_type, spatial_reference=SR)

    arcpy.management.AddField(fc, "VEGLENKESEKV_ID", "LONG")
    arcpy.management.AddField(fc, "STARTPOS", "DOUBLE")
    arcpy.management.AddField(fc, "SLUTTPOS", "DOUBLE")

    for f in extra_fields:
        if len(f) == 2:
            arcpy.management.AddField(fc, f[0], f[1])
        else:
            arcpy.management.AddField(fc, f[0], f[1], field_length=f[2])

    return fc


# -------------------------
# Kjøreloggfelter + domain + init status
# -------------------------
def legg_til_kjoreloggfelter(fc: str) -> None:
    wanted = [
        ("Status_Måling", "TEXT", 30),
        ("Måledato", "DATE", None),
        ("Målebiloperatør", "TEXT", 50),
        ("Målebil", "TEXT", 50),
        ("Kommentar_MBO", "TEXT", 255),
        ("Driftskontrakt", "TEXT", 150),
        ("Lengde_km", "DOUBLE", None),
        ("KEY_DISS", "TEXT", 220),  # komposittnøkkel for median-join
    ]
    eksisterende = {f.name for f in arcpy.ListFields(fc)}
    for navn, ftype, lengde in wanted:
        if navn in eksisterende:
            continue
        if ftype == "TEXT" and lengde:
            arcpy.management.AddField(fc, navn, ftype, field_length=lengde)
        else:
            arcpy.management.AddField(fc, navn, ftype)


def setup_domain(gdb: str) -> None:
    domain = "StatusMal_Domain"
    domains = [d.name for d in arcpy.da.ListDomains(gdb)]
    if domain not in domains:
        arcpy.management.CreateDomain(gdb, domain, "Status måling", "TEXT", "CODED")
        for v in ["IKKE MÅLT", "PÅBEGYNT", "MÅLES IKKE", "FERDIG MÅLT"]:
            arcpy.management.AddCodedValueToDomain(gdb, domain, v, v)
        log("✓ Domain opprettet")

    arcpy.env.workspace = gdb
    for fc in arcpy.ListFeatureClasses():
        fnames = {f.name for f in arcpy.ListFields(fc)}
        if "Status_Måling" in fnames:
            arcpy.management.AssignDomainToField(fc, "Status_Måling", domain)


def init_status_ikke_malt(fc: str) -> None:
    fields = {f.name for f in arcpy.ListFields(fc)}
    if "Status_Måling" not in fields:
        return
    arcpy.management.CalculateField(fc, "Status_Måling", "'IKKE MÅLT'", "PYTHON3")


# -------------------------
# Driftskontrakt
# -------------------------
def importer_driftskontrakt(gdb: str) -> str:
    src = os.path.join(DRIFTSKONTRAKT_GPKG, DRIFTSKONTRAKT_LAYER)
    if not arcpy.Exists(src):
        raise RuntimeError(f"Fant ikke driftskontrakt-lag: {src}")
    dst = os.path.join(gdb, "Driftskontrakt")
    arcpy.conversion.FeatureClassToFeatureClass(src, gdb, "Driftskontrakt")
    return dst


def spatial_join_driftskontrakt(target_fc: str, drift_fc: str) -> str:
    out_fc = target_fc + "_join"
    arcpy.analysis.SpatialJoin(
        target_features=target_fc,
        join_features=drift_fc,
        out_feature_class=out_fc,
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        match_option="INTERSECT",
    )
    arcpy.management.Delete(target_fc)
    arcpy.management.Rename(out_fc, target_fc)
    return target_fc


def calc_driftskontrakt(target_fc: str) -> None:
    fields = {f.name for f in arcpy.ListFields(target_fc)}
    if KONTRAKT_FELT not in fields:
        raise RuntimeError(
            f"Finner ikke felt '{KONTRAKT_FELT}' etter join. "
            "Sjekk at polygonlaget har feltet og at SpatialJoin treffer."
        )
    arcpy.management.CalculateField(target_fc, "Driftskontrakt", f"!{KONTRAKT_FELT}!", "PYTHON3")


# -------------------------
# Lengde
# -------------------------
def beregn_lengde_km(fc: str) -> None:
    # ArcPy parameter: Coordinate_System (stor C/S)
    arcpy.management.AddGeometryAttributes(
        fc,
        "LENGTH_GEODESIC",
        "KILOMETERS",
        Coordinate_System=SR,
    )
    fields = [f.name for f in arcpy.ListFields(fc)]
    src_field = "LENGTH_GEODESIC" if "LENGTH_GEODESIC" in fields else ("LENGTHGEODESIC" if "LENGTHGEODESIC" in fields else None)
    if src_field:
        if "Lengde_km" in fields:
            arcpy.management.CalculateField(fc, "Lengde_km", f"!{src_field}!", "PYTHON3")
        else:
            arcpy.management.AlterField(fc, src_field, "Lengde_km", "Lengde_km")


# -------------------------
# KEY for dissolve/median
# -------------------------
def calc_key_diss(fc: str) -> None:
    fields = {f.name for f in arcpy.ListFields(fc)}
    if not {"TRAFIKANTGRP", "VEGKATEGORI", "VEGNUMMER", "Driftskontrakt", "KEY_DISS"} <= fields:
        raise RuntimeError("Mangler felt for KEY_DISS. Har du kjørt legg_til_kjoreloggfelter + join drift?")

    expr = (
        "!TRAFIKANTGRP! + '|' + str(!VEGKATEGORI!) + '|' + str(!VEGNUMMER!) + '|' + "
        "(str(!Driftskontrakt!) if !Driftskontrakt! else '')"
    )
    arcpy.management.CalculateField(fc, "KEY_DISS", expr, "PYTHON3")


# -------------------------
# 1) VEGNETT – segmentert fra NVDB
# -------------------------
def hent_vegnett_segmentert(
    session: requests.Session,
    gdb: str,
    *,
    out_name: str,
    fylke: int,
    vegsystemref: str,
    trafikantgruppe: str,
    kommune: Optional[int] = None,
) -> str:
    fc = create_fc(
        gdb,
        out_name,
        "POLYLINE",
        [
            ("TRAFIKANTGRP", "TEXT", 1),
            ("VEGKATEGORI", "TEXT", 1),
            ("VEGNUMMER", "LONG"),
            ("VEGREF", "TEXT", 50),
            ("KOMMUNE", "TEXT", 60),
            ("FYLKE_NAVN", "TEXT", 40),
        ],
    )

    url = f"{VEGNETT_API}/veglenkesekvenser/segmentert"
    params: Dict[str, Any] = {
        "fylke": fylke,
        "vegsystemreferanse": vegsystemref,
        "antall": 5000,
        "inkluderAntall": "false",
        "srid": SRID,
    }
    if kommune is not None:
        params["kommune"] = kommune

    cols = [
        "SHAPE@",
        "VEGLENKESEKV_ID",
        "STARTPOS",
        "SLUTTPOS",
        "TRAFIKANTGRP",
        "VEGKATEGORI",
        "VEGNUMMER",
        "VEGREF",
        "KOMMUNE",
        "FYLKE_NAVN",
    ]

    cnt = 0
    with arcpy.da.InsertCursor(fc, cols) as cur:
        for seg in iter_paged(session, url, params, label=f"vegnett_{out_name}"):
            vr = seg.get("vegsystemreferanse", {}) or {}
            tg = (vr.get("strekning", {}) or {}).get("trafikantgruppe")
            if tg != trafikantgruppe:
                continue

            geom = to_geometry(seg.get("geometri"))
            if not geom:
                continue

            vs = vr.get("vegsystem", {}) or {}
            stre = vr.get("strekning", {}) or {}

            vegref = None
            if vs.get("vegkategori") and vs.get("nummer"):
                vegref = f"{vs['vegkategori']}V{vs['nummer']}"
                if stre.get("strekning") and stre.get("delstrekning"):
                    vegref += f" S{stre['strekning']}D{stre['delstrekning']}"

            loc = seg.get("lokasjon") or {}
            kommune_s = str(loc["kommuner"][0]) if loc.get("kommuner") else None
            fylke_s = str(loc["fylker"][0]) if loc.get("fylker") else None

            cur.insertRow(
                (
                    geom,
                    int(seg["veglenkesekvensid"]),
                    float(seg.get("startposisjon", 0.0)),
                    float(seg.get("sluttposisjon", 0.0)),
                    tg,
                    vs.get("vegkategori"),
                    vs.get("nummer"),
                    vegref,
                    kommune_s,
                    fylke_s,
                )
            )
            cnt += 1

    log(f"✓ {out_name}: {cnt} segmenter")
    return fc


# -------------------------
# 2) REGNETT – dissolve + median
# -------------------------
def lag_regnett_med_median(gdb: str, in_fc: str, out_name: str) -> str:
    out_fc = os.path.join(gdb, out_name)

    # Sørg for lengde på segmenter
    beregn_lengde_km(in_fc)

    # Key for grouping
    calc_key_diss(in_fc)

    # Median-tabell pr KEY_DISS
    stats_tbl = os.path.join(gdb, f"tbl_median_{out_name}")
    if arcpy.Exists(stats_tbl):
        arcpy.management.Delete(stats_tbl)

    arcpy.analysis.Statistics(
        in_table=in_fc,
        out_table=stats_tbl,
        statistics_fields=[["Lengde_km", "MEDIAN"]],
        case_field=["KEY_DISS"],
    )

    # Dissolve geometri
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)

    dissolve_fields = ["TRAFIKANTGRP", "VEGKATEGORI", "VEGNUMMER", "Driftskontrakt", "KEY_DISS"]
    arcpy.management.Dissolve(
        in_features=in_fc,
        out_feature_class=out_fc,
        dissolve_field=dissolve_fields,
        statistics_fields=None,
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES",
    )

    # Legg på felt + join median
    fields_tbl = {f.name for f in arcpy.ListFields(stats_tbl)}
    median_field = "MEDIAN_Lengde_km" if "MEDIAN_Lengde_km" in fields_tbl else None
    if not median_field:
        raise RuntimeError(f"Fant ikke median-felt i {stats_tbl}. Felter: {sorted(fields_tbl)}")

    fields_out = {f.name for f in arcpy.ListFields(out_fc)}
    if "Lengde_km_median" not in fields_out:
        arcpy.management.AddField(out_fc, "Lengde_km_median", "DOUBLE")

    arcpy.management.JoinField(out_fc, "KEY_DISS", stats_tbl, "KEY_DISS", [median_field])
    arcpy.management.CalculateField(out_fc, "Lengde_km_median", f"!{median_field}!", "PYTHON3")

    # Kjøreloggfelter + domain + init status
    legg_til_kjoreloggfelter(out_fc)
    setup_domain(gdb)
    init_status_ikke_malt(out_fc)

    return out_fc


# -------------------------
# MAIN
# -------------------------
def main() -> None:
    log("🚀 Kjørelogg 2026 – bygger FV vegnett + registreringsnett (median)")

    os.makedirs(OUT_FOLDER, exist_ok=True)
    create_gdb(OUT_GDB)
    arcpy.env.workspace = OUT_GDB

    session = create_session()

    # Import driftskontrakt
    drift_fc = importer_driftskontrakt(OUT_GDB)
    log("✓ Driftskontrakt importert")

    for tg in ("K", "G"):
        log(f"\n=== Trafikantgruppe {tg} ===")

        # MR
        mr_fc = hent_vegnett_segmentert(
            session,
            OUT_GDB,
            out_name=f"tmp_MR_{tg}",
            fylke=MR_FYLKE,
            vegsystemref=VEGSYSTEMREF_MR,
            trafikantgruppe=tg,
        )

        # Vestland FV61 (Stad)
        vl_fc = hent_vegnett_segmentert(
            session,
            OUT_GDB,
            out_name=f"tmp_VL61_{tg}",
            fylke=VL_FYLKE,
            kommune=VL_KOMM,
            vegsystemref=VEGSYSTEMREF_VL,
            trafikantgruppe=tg,
        )

        # Merge detalj
        base_fc = os.path.join(OUT_GDB, f"Vegnett_FV_{tg}")
        if arcpy.Exists(base_fc):
            arcpy.management.Delete(base_fc)
        arcpy.management.Merge([mr_fc, vl_fc], base_fc)
        arcpy.management.Delete(mr_fc)
        arcpy.management.Delete(vl_fc)

        # Felter + domain
        legg_til_kjoreloggfelter(base_fc)
        setup_domain(OUT_GDB)

        # Join driftskontrakt + beregn + init status
        base_fc = spatial_join_driftskontrakt(base_fc, drift_fc)
        calc_driftskontrakt(base_fc)
        beregn_lengde_km(base_fc)
        init_status_ikke_malt(base_fc)

        n = int(arcpy.management.GetCount(base_fc)[0])
        log(f"✅ {os.path.basename(base_fc)}: {n} rader")

        # Registreringsnett (propagering) – median
        reg_fc = lag_regnett_med_median(OUT_GDB, base_fc, f"Regnett_FV_{tg}")
        nr = int(arcpy.management.GetCount(reg_fc)[0])
        log(f"✅ {os.path.basename(reg_fc)}: {nr} rader")

    log("\n🎉 FERDIG")
    log(OUT_GDB)
    log("Lag:")
    log("  - Vegnett_FV_K / Vegnett_FV_G (detalj)")
    log("  - Regnett_FV_K / Regnett_FV_G (registrering, dissolve + median)")
    log("Status_Måling er initialisert til 'IKKE MÅLT' for alle rader.")


if __name__ == "__main__":
    main()