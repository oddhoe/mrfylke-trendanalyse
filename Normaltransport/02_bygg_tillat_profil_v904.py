# -*- coding: utf-8 -*-
"""
02_bygg_tillat_profil_v904.py

Bygger Veg_TillatProfil (segmentnivå) med tillatte dimensjoner for NORMALTRANSPORT basert på:
  - Vegnett (FV, fylke 15)           : feature class "Vegnett"
  - Bruksklasse 904 (normaltransport): feature class "Bruksklasse_904"
  - Bruer (60)                       : feature class "Bruer"
  - Høydebegrensning (591)           : feature class "Hoydebegrensning_591"

Output:
  - Veg_TillatProfil (POLYLINE) med felter:
      VEGLENKESEKV_ID (LONG)
      STARTPOS        (DOUBLE)
      SLUTTPOS        (DOUBLE)
      BK_VERDI        (LONG)    # tolket tonn fra BK-tekst
      BK_TEKST        (TEXT)
      MAKS_LENGDE     (DOUBLE)  # meter — fra "skiltet" verdi for Spes, ellers enum-verdi
      ER_SPES         (TEXT)    # "JA" hvis segment har Spesiell begrensning (18256)
      MIN_BRU_TONN    (LONG)    # tonn (minste bru som overlapper segment)
      MIN_HOYDE       (DOUBLE)  # meter (minste skiltede høyde som overlapper segment)
      TILLATT_TONN    (LONG)    # min(BK_VERDI, MIN_BRU_TONN) med None-håndtering
      REGIME          (TEXT)    # "NORMALTRANSPORT"
      BK_OBJTYPE      (LONG)    # 904

Endringer:
  - Leser ER_SPES fra Bruksklasse_904 (satt av oppdatert nvdb_to_gdb).
  - BK-overlapp bruker strict=False (fanger kantberøring).
  - ER_SPES propageres til output for sporbarhet i nedstrøms-steg.
  - Debug-melding viser antall segmenter uten MAKS_LENGDE etter kjøring.
"""

from __future__ import annotations
import os
import arcpy

arcpy.env.overwriteOutput = True

# ------------------------------
# KONFIG
# ------------------------------
GDB        = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"
FC_VEGNETT = os.path.join(GDB, "Vegnett")
FC_BK      = os.path.join(GDB, "Bruksklasse_904")
FC_BRU     = os.path.join(GDB, "Bruer")
FC_HOY     = os.path.join(GDB, "Hoydebegrensning_591")
OUT_FC     = os.path.join(GDB, "Veg_TillatProfil")

ID_FIELD       = "VEGLENKESEKV_ID"
S0             = "STARTPOS"
S1             = "SLUTTPOS"
STRICT_OVERLAP = True   # brukes for bru og høyde
EPS            = 1e-9


# ------------------------------
# HJELPEFUNKSJONER
# ------------------------------
def overlap(a0, a1, b0, b1, strict=True):
    left  = max(a0, b0)
    right = min(a1, b1)
    if strict:
        return left < right - EPS
    return left <= right + EPS


def min_or_none(vals):
    v = [x for x in vals if x is not None]
    return min(v) if v else None


def ensure_field(fc, name, ftype, length=None):
    existing = {f.name for f in arcpy.ListFields(fc)}
    if name in existing:
        return
    if length is None:
        arcpy.management.AddField(fc, name, ftype)
    else:
        arcpy.management.AddField(fc, name, ftype, field_length=length)


def require_fc(path):
    if not arcpy.Exists(path):
        raise RuntimeError(f"Mangler feature class: {path}")


# ------------------------------
# VALIDÉR INPUT
# ------------------------------
for p in [FC_VEGNETT, FC_BK, FC_BRU, FC_HOY]:
    require_fc(p)


def check_fields(fc, needed):
    fields  = {f.name for f in arcpy.ListFields(fc)}
    missing = [n for n in needed if n not in fields]
    if missing:
        raise RuntimeError(f"{os.path.basename(fc)} mangler felt: {missing}")


check_fields(FC_VEGNETT, [ID_FIELD, S0, S1])
check_fields(FC_BK,  [ID_FIELD, S0, S1, "BK_VERDI", "BK_TEKST", "MAKS_LENGDE"])
check_fields(FC_BRU, [ID_FIELD, S0, S1, "TILLATT_TONN"])
check_fields(FC_HOY, [ID_FIELD, S0, S1, "SKILTET_HOYDE"])

# ER_SPES er valgfritt (finnes kun om nvdb_to_gdb er oppdatert)
bk_fields_in = {f.name for f in arcpy.ListFields(FC_BK)}
has_er_spes  = "ER_SPES" in bk_fields_in

# ------------------------------
# BYGG OPPSLAG (per VEGLENKESEKV_ID)
# ------------------------------
arcpy.AddMessage("Bygger oppslag (BK 904 / Bru / Høyde) per VEGLENKESEKV_ID...")

# idx_bk: vid -> list[(s0, s1, bk_val, bk_text, maks_len, er_spes)]
idx_bk = {}
bk_read = [ID_FIELD, S0, S1, "BK_VERDI", "BK_TEKST", "MAKS_LENGDE"]
if has_er_spes:
    bk_read.append("ER_SPES")

with arcpy.da.SearchCursor(FC_BK, bk_read) as cur:
    for row in cur:
        vid, a0, a1, bk, txt, ml = row[0], row[1], row[2], row[3], row[4], row[5]
        er_spes = row[6] if has_er_spes else "NEI"
        if vid is None:
            continue
        idx_bk.setdefault(int(vid), []).append(
            (float(a0 or 0), float(a1 or 0), bk, txt, ml, er_spes)
        )

idx_bru = {}
with arcpy.da.SearchCursor(FC_BRU, [ID_FIELD, S0, S1, "TILLATT_TONN"]) as cur:
    for vid, a0, a1, tonn in cur:
        if vid is None:
            continue
        idx_bru.setdefault(int(vid), []).append(
            (float(a0 or 0), float(a1 or 0), tonn)
        )

idx_hoy = {}
with arcpy.da.SearchCursor(FC_HOY, [ID_FIELD, S0, S1, "SKILTET_HOYDE"]) as cur:
    for vid, a0, a1, h in cur:
        if vid is None:
            continue
        idx_hoy.setdefault(int(vid), []).append(
            (float(a0 or 0), float(a1 or 0), h)
        )

arcpy.AddMessage(
    f"BK-lenker: {len(idx_bk)}, Bru-lenker: {len(idx_bru)}, Høyde-lenker: {len(idx_hoy)}"
)

# ------------------------------
# OPPRETT OUTPUT
# ------------------------------
arcpy.AddMessage("Oppretter Veg_TillatProfil...")

if arcpy.Exists(OUT_FC):
    arcpy.management.Delete(OUT_FC)

arcpy.management.CreateFeatureclass(
    out_path=os.path.dirname(OUT_FC),
    out_name=os.path.basename(OUT_FC),
    geometry_type="POLYLINE",
    spatial_reference=FC_VEGNETT,
)

ensure_field(OUT_FC, ID_FIELD,       "LONG")
ensure_field(OUT_FC, S0,             "DOUBLE")
ensure_field(OUT_FC, S1,             "DOUBLE")
ensure_field(OUT_FC, "BK_VERDI",     "LONG")
ensure_field(OUT_FC, "BK_TEKST",     "TEXT",   length=120)
ensure_field(OUT_FC, "MAKS_LENGDE",  "DOUBLE")
ensure_field(OUT_FC, "ER_SPES",      "TEXT",   length=5)   # ny
ensure_field(OUT_FC, "MIN_BRU_TONN", "LONG")
ensure_field(OUT_FC, "MIN_HOYDE",    "DOUBLE")
ensure_field(OUT_FC, "TILLATT_TONN", "LONG")
ensure_field(OUT_FC, "REGIME",       "TEXT",   length=30)
ensure_field(OUT_FC, "BK_OBJTYPE",   "LONG")

# ------------------------------
# FYLL OUTPUT
# ------------------------------
out_cols = [
    "SHAPE@", ID_FIELD, S0, S1,
    "BK_VERDI", "BK_TEKST", "MAKS_LENGDE", "ER_SPES",
    "MIN_BRU_TONN", "MIN_HOYDE", "TILLATT_TONN",
    "REGIME", "BK_OBJTYPE",
]

veg_cols     = ["SHAPE@", ID_FIELD, S0, S1]
count        = 0
none_len_cnt = 0
spes_cnt     = 0

with arcpy.da.InsertCursor(OUT_FC, out_cols) as icur:
    with arcpy.da.SearchCursor(FC_VEGNETT, veg_cols) as vcur:
        for geom, vid, v0, v1 in vcur:
            if vid is None:
                continue
            vid = int(vid)
            v0  = float(v0 or 0.0)
            v1  = float(v1 or 0.0)

            # --- BK (904): strict=False for å fange kantberøring ---
            hits_bk = []
            for a0, a1, bk_val, bk_txt, maks_len, er_spes in idx_bk.get(vid, []):
                if overlap(v0, v1, a0, a1, strict=False):
                    hits_bk.append((bk_val, bk_txt, maks_len, er_spes))

            bk_val   = min_or_none([h[0] for h in hits_bk])
            bk_txt   = next((h[1] for h in hits_bk if h[1]), None)
            maks_len = min_or_none([h[2] for h in hits_bk])

            # ER_SPES = "JA" hvis minst ett overlappende segment er Spes
            er_spes_out = "JA" if any(h[3] == "JA" for h in hits_bk) else "NEI"

            if maks_len is None:
                none_len_cnt += 1
            if er_spes_out == "JA":
                spes_cnt += 1

            # --- Bru: minste tillatte tonn ---
            hits_bru = []
            for a0, a1, bru_tonn in idx_bru.get(vid, []):
                if overlap(v0, v1, a0, a1, strict=STRICT_OVERLAP):
                    hits_bru.append(bru_tonn)
            min_bru = min_or_none(hits_bru)

            # --- Høyde: punkt-objekter, tillat touch ---
            hits_h = []
            for a0, a1, hoyde in idx_hoy.get(vid, []):
                if overlap(v0, v1, a0, a1, strict=False):
                    hits_h.append(hoyde)
            min_h = min_or_none(hits_h)

            # --- TILLATT_TONN = min(bk_val, min_bru) ---
            if bk_val is None and min_bru is None:
                tillatt = None
            elif bk_val is None:
                tillatt = min_bru
            elif min_bru is None:
                tillatt = bk_val
            else:
                tillatt = min(bk_val, min_bru)

            icur.insertRow((
                geom, vid, v0, v1,
                bk_val, bk_txt, maks_len, er_spes_out,
                min_bru, min_h, tillatt,
                "NORMALTRANSPORT", 904,
            ))
            count += 1

arcpy.AddMessage(f"✅ Ferdig Veg_TillatProfil: {count} segmenter")
arcpy.AddMessage(
    f"   Segmenter uten MAKS_LENGDE : {none_len_cnt}"
)
arcpy.AddMessage(
    f"   Herav Spes (ER_SPES=JA)    : {spes_cnt}  "
    f"({'MAKS_LENGDE satt fra skiltet-felt' if spes_cnt and none_len_cnt < spes_cnt else 'sjekk om skiltet-felt er registrert i NVDB'})"
)
