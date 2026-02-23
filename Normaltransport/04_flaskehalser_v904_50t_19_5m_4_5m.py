# -*- coding: utf-8 -*-
"""
04_flaskehalser_v904_50t_19_5m_4_5m.py

Finner flaskehalser fra Veg_TillatSegmentert for NORMALTRANSPORT:
  - TONN_PROP  < 50    (vekt; propagert min per veglenke)
  - MIN_BRU_TONN < 60  (bru alene; uavhengig av BK-verdi)
  - LEN_PROP   < 19.5  (lengde; propagert min per veglenke)
  - HOY_PROP   < 4.5   (høyde; propagert min per veglenke)

Leses fra Veg_TillatSegmentert (output fra steg 03) som inneholder propagerte
minimumsverdier per VEGLENKESEKV_ID – dette sikrer at f.eks. Trollstigen (13,3 m)
fanges selv om enkelt-segmentet ikke har BK-treff.

Output: Flaskehalser_BK904_Normal_50t_19_5m_4_5m
"""

import arcpy
import os

arcpy.env.overwriteOutput = True

# ------------------------------
# KONFIG
# ------------------------------
GDB    = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"
IN_FC  = os.path.join(GDB, "Veg_TillatSegmentert")   # <-- endret fra Veg_TillatProfil
OUT_FC = os.path.join(GDB, "Flaskehalser_BK904_Normal_50t_19_5m_4_5m")

# --- KRAV ---
VEKT_KRAV     = 50.0   # tonn (BK/normaltransport)
BRU_TONN_KRAV = 60.0   # tonn – bru under 60t er selvstendig flaskehals
LENGDE_KRAV   = 19.5   # meter
HOYDE_KRAV    = 4.5    # meter

ID_FIELD = "VEGLENKESEKV_ID"

# ------------------------------
# OPPRETT OUTPUT
# ------------------------------
path, name = os.path.split(OUT_FC)
if arcpy.Exists(OUT_FC):
    arcpy.management.Delete(OUT_FC)
sr = arcpy.Describe(IN_FC).spatialReference
arcpy.management.CreateFeatureclass(path, name, "POLYLINE", spatial_reference=sr)


arcpy.management.AddField(OUT_FC, ID_FIELD,           "LONG")
arcpy.management.AddField(OUT_FC, "STARTPOS",         "DOUBLE")
arcpy.management.AddField(OUT_FC, "SLUTTPOS",         "DOUBLE")
arcpy.management.AddField(OUT_FC, "TILLATT_TONN",     "DOUBLE")
arcpy.management.AddField(OUT_FC, "MIN_BRU_TONN",     "DOUBLE")   # ny
arcpy.management.AddField(OUT_FC, "MAKS_LENGDE",      "DOUBLE")
arcpy.management.AddField(OUT_FC, "FRI_HOYDE",        "DOUBLE")
arcpy.management.AddField(OUT_FC, "FLASKEHALS",       "TEXT",  field_length=5)
arcpy.management.AddField(OUT_FC, "BEGRENSNING_TYPE", "TEXT",  field_length=60)
arcpy.management.AddField(OUT_FC, "BESKRIVELSE",      "TEXT",  field_length=250)
arcpy.management.AddField(OUT_FC, "REGIME",           "TEXT",  field_length=30)
arcpy.management.AddField(OUT_FC, "BK_OBJTYPE",       "LONG")

# ------------------------------
# FELTDETEKSJON I INPUT
# ------------------------------
in_fields = {f.name for f in arcpy.ListFields(IN_FC)}

# Propagerte felt (foretrukket) med fallback til originale felt
has_tonn_prop = "TONN_PROP"    in in_fields
has_len_prop  = "LEN_PROP"     in in_fields
has_hoy_prop  = "HOY_PROP"     in in_fields
has_tonn_orig = "TILLATT_TONN" in in_fields   # fallback
has_len_orig  = "MAKS_LENGDE"  in in_fields   # fallback
has_hoy_orig  = "MIN_HOYDE"    in in_fields   # fallback
has_bru       = "MIN_BRU_TONN" in in_fields
has_regime    = "REGIME"       in in_fields
has_bkobj     = "BK_OBJTYPE"   in in_fields

# Velg felt vi faktisk leser, med propagerte som primær
USE_TONN   = "TONN_PROP"    if has_tonn_prop else ("TILLATT_TONN" if has_tonn_orig else None)
USE_LENGDE = "LEN_PROP"     if has_len_prop  else ("MAKS_LENGDE"  if has_len_orig  else None)
USE_HOYDE  = "HOY_PROP"     if has_hoy_prop  else ("MIN_HOYDE"    if has_hoy_orig  else None)

if USE_TONN is None:
    raise RuntimeError("Finner verken TONN_PROP eller TILLATT_TONN i input. Kjør steg 03 først.")

read_fields = ["SHAPE@", ID_FIELD, "STARTPOS", "SLUTTPOS", USE_TONN]
if USE_LENGDE:  read_fields.append(USE_LENGDE)
if USE_HOYDE:   read_fields.append(USE_HOYDE)
if has_bru:     read_fields.append("MIN_BRU_TONN")
if has_regime:  read_fields.append("REGIME")
if has_bkobj:   read_fields.append("BK_OBJTYPE")

cols_out = [
    "SHAPE@", ID_FIELD, "STARTPOS", "SLUTTPOS",
    "TILLATT_TONN", "MIN_BRU_TONN", "MAKS_LENGDE", "FRI_HOYDE",
    "FLASKEHALS", "BEGRENSNING_TYPE", "BESKRIVELSE",
    "REGIME", "BK_OBJTYPE",
]

print(f"Leser fra: {os.path.basename(IN_FC)}")
print(f"  Vekt-felt  : {USE_TONN}")
print(f"  Lengde-felt: {USE_LENGDE or '(mangler)'}")
print(f"  Høyde-felt : {USE_HOYDE  or '(mangler)'}")
print(f"  Bru-felt   : {'MIN_BRU_TONN' if has_bru else '(mangler)'}")

# ------------------------------
# FYLL OUTPUT
# ------------------------------
count = 0

with arcpy.da.InsertCursor(OUT_FC, cols_out) as icur:
    with arcpy.da.SearchCursor(IN_FC, read_fields) as scur:
        for row in scur:
            geom = row[0]
            vid  = row[1]
            s0   = row[2]
            s1   = row[3]
            vekt = row[4]   # TONN_PROP eller TILLATT_TONN

            idx = 5
            lengde  = row[idx] if USE_LENGDE else None
            idx    += 1 if USE_LENGDE else 0

            hoyde   = row[idx] if USE_HOYDE  else None
            idx    += 1 if USE_HOYDE  else 0

            min_bru = row[idx] if has_bru    else None
            idx    += 1 if has_bru    else 0

            regime  = row[idx] if has_regime else "NORMALTRANSPORT"
            idx    += 1 if has_regime else 0

            bkobj   = row[idx] if has_bkobj  else 904

            # --- EVALUER BEGRENSNINGER ---
            feil  = []
            typer = []

            # 1) Vekt < 50 tonn (BK og/eller bru som dimensjonerende)
            if vekt is not None and float(vekt) < VEKT_KRAV:
                feil.append(f"Vekt ({vekt}t < {VEKT_KRAV}t)")
                typer.append("Vekt")

            # 2) Bru < 60 tonn (selvstendig begrensning uavhengig av BK)
            if min_bru is not None and float(min_bru) < BRU_TONN_KRAV:
                feil.append(f"Bru ({min_bru}t < {BRU_TONN_KRAV}t)")
                typer.append("Bru60")

            # 3) Lengde < 19,5 m
            if lengde is not None and float(lengde) < LENGDE_KRAV:
                feil.append(f"Lengde ({lengde}m < {LENGDE_KRAV}m)")
                typer.append("Lengde")

            # 4) Høyde < 4,5 m
            if hoyde is not None and float(hoyde) < HOYDE_KRAV:
                feil.append(f"Høyde ({hoyde}m < {HOYDE_KRAV}m)")
                typer.append("Høyde")

            if not feil:
                continue

            begr_type  = " og ".join(typer)
            beskrivelse = ", ".join(feil)

            icur.insertRow((
                geom, vid, s0, s1,
                vekt, min_bru, lengde, hoyde,
                "JA", begr_type, beskrivelse,
                regime, bkobj,
            ))
            count += 1

print(f"✅ Ferdig! Fant {count} flaskehals-segmenter.")
