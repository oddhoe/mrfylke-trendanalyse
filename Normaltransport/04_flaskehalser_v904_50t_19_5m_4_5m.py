# -*- coding: utf-8 -*-
# 04_flaskehalser_v904_50t_19_5m_4_5m.py
#
# Finner flaskehalser fra Veg_TillatProfil for NORMALTRANSPORT:
# - TILLATT_TONN < 50
# - MAKS_LENGDE < 19.5
# - MIN_HOYDE < 4.5
#
# Output: Flaskehalser_BK904_Normal_50t_19_5m_4_5m

import arcpy
import os

arcpy.env.overwriteOutput = True

GDB = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"
IN_FC = os.path.join(GDB, "Veg_TillatProfil")
OUT_FC = os.path.join(GDB, "Flaskehalser_BK904_Normal_50t_19_5m_4_5m")

# --- KRAV ---
VEKT_KRAV = 50.0      # tonn
LENGDE_KRAV = 19.5    # meter
HOYDE_KRAV = 4.5      # meter

ID_FIELD = "VEGLENKESEKV_ID"

# Opprett output
path, name = os.path.split(OUT_FC)
if arcpy.Exists(OUT_FC):
    arcpy.management.Delete(OUT_FC)
arcpy.management.CreateFeatureclass(path, name, "POLYLINE", spatial_reference=IN_FC)

# Felter
arcpy.management.AddField(OUT_FC, ID_FIELD, "LONG")
arcpy.management.AddField(OUT_FC, "STARTPOS", "DOUBLE")
arcpy.management.AddField(OUT_FC, "SLUTTPOS", "DOUBLE")
arcpy.management.AddField(OUT_FC, "TILLATT_TONN", "DOUBLE")
arcpy.management.AddField(OUT_FC, "MAKS_LENGDE", "DOUBLE")
arcpy.management.AddField(OUT_FC, "FRI_HOYDE", "DOUBLE")
arcpy.management.AddField(OUT_FC, "FLASKEHALS", "TEXT", field_length=5)
arcpy.management.AddField(OUT_FC, "BEGRENSNING_TYPE", "TEXT", field_length=50)
arcpy.management.AddField(OUT_FC, "BESKRIVELSE", "TEXT", field_length=200)
arcpy.management.AddField(OUT_FC, "REGIME", "TEXT", field_length=30)
arcpy.management.AddField(OUT_FC, "BK_OBJTYPE", "LONG")

# Sjekk felter i input
in_fields = {f.name for f in arcpy.ListFields(IN_FC)}
has_len = "MAKS_LENGDE" in in_fields
has_hoy = "MIN_HOYDE" in in_fields
has_regime = "REGIME" in in_fields
has_bkobj = "BK_OBJTYPE" in in_fields

read_fields = ["SHAPE@", ID_FIELD, "STARTPOS", "SLUTTPOS", "TILLATT_TONN"]
if has_len: read_fields.append("MAKS_LENGDE")
if has_hoy: read_fields.append("MIN_HOYDE")
if has_regime: read_fields.append("REGIME")
if has_bkobj: read_fields.append("BK_OBJTYPE")

cols_out = [
    "SHAPE@", ID_FIELD, "STARTPOS", "SLUTTPOS",
    "TILLATT_TONN", "MAKS_LENGDE", "FRI_HOYDE",
    "FLASKEHALS", "BEGRENSNING_TYPE", "BESKRIVELSE",
    "REGIME", "BK_OBJTYPE"
]

count = 0
with arcpy.da.InsertCursor(OUT_FC, cols_out) as icur:
    with arcpy.da.SearchCursor(IN_FC, read_fields) as scur:
        for row in scur:
            geom = row[0]
            vid = row[1]
            s0 = row[2]
            s1 = row[3]
            vekt = row[4]

            idx = 5
            lengde = row[idx] if has_len else None
            idx += 1 if has_len else 0
            hoyde = row[idx] if has_hoy else None
            idx += 1 if has_hoy else 0
            regime = row[idx] if has_regime else "NORMALTRANSPORT"
            idx += 1 if has_regime else 0
            bkobj = row[idx] if has_bkobj else 904

            feil = []
            typer = []

            if vekt is not None and float(vekt) < VEKT_KRAV:
                feil.append(f"Vekt ({vekt}t)")
                typer.append("Vekt")

            if lengde is not None and float(lengde) < LENGDE_KRAV:
                feil.append(f"Lengde ({lengde}m)")
                typer.append("Lengde")

            if hoyde is not None and float(hoyde) < HOYDE_KRAV:
                feil.append(f"Høyde ({hoyde}m)")
                typer.append("Høyde")

            if not feil:
                continue

            begr_type = " og ".join(typer)
            beskrivelse = ", ".join(feil)

            icur.insertRow((
                geom, vid, s0, s1,
                vekt, lengde, hoyde,
                "JA", begr_type, beskrivelse,
                regime, bkobj
            ))
            count += 1

print(f"✅ Ferdig! Fant {count} flaskehals-segmenter.")
