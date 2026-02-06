# 04_flaskehalser_bk900_FIXED.py
# ✅ FIXED: Bedre tekst-formattering for BEGRENSNING_TYPE

import arcpy
import os

arcpy.env.overwriteOutput = True

GDB = r"D:\Conda\Flaskehalser\gdb\nvdb_radata.gdb"
IN_FC = os.path.join(GDB, "Veg_TillatProfil")
OUT_FC = os.path.join(GDB, "Flaskehalser_BK900")

# --- KRAV ---
VEKT_KRAV = 50      # tonn
LENGDE_KRAV = 24.0  # meter 
HOYDE_KRAV = 4.5    # meter

ID_FIELD = "VEGLENKESEKV_ID"

print("Oppretter output feature class...")

path, name = os.path.split(OUT_FC)
if arcpy.Exists(OUT_FC): arcpy.management.Delete(OUT_FC)
arcpy.management.CreateFeatureclass(path, name, "POLYLINE", spatial_reference=IN_FC)

# Opprett felter
arcpy.management.AddField(OUT_FC, ID_FIELD, "LONG")
arcpy.management.AddField(OUT_FC, "STARTPOS", "DOUBLE")
arcpy.management.AddField(OUT_FC, "SLUTTPOS", "DOUBLE")
arcpy.management.AddField(OUT_FC, "TILLATT_TONN", "LONG")
arcpy.management.AddField(OUT_FC, "MAKS_LENGDE", "DOUBLE")
arcpy.management.AddField(OUT_FC, "FRI_HOYDE", "DOUBLE")
arcpy.management.AddField(OUT_FC, "FLASKEHALS", "TEXT", field_length=5)
arcpy.management.AddField(OUT_FC, "BEGRENSNING_TYPE", "TEXT", field_length=100) # Økt lengde for sikkerhets skyld
arcpy.management.AddField(OUT_FC, "BESKRIVELSE", "TEXT", field_length=200)

# Sjekk input-felter for å unngå krasj hvis lengde/høyde mangler i steg 2
in_fields_map = {f.name: f for f in arcpy.ListFields(IN_FC)}
has_lengde = "MAKS_LENGDE" in in_fields_map
has_hoyde = "MIN_HOYDE" in in_fields_map

read_fields = ["SHAPE@", ID_FIELD, "TILLATT_TONN", "STARTPOS", "SLUTTPOS"]
if has_lengde: read_fields.append("MAKS_LENGDE")
if has_hoyde: read_fields.append("MIN_HOYDE")

print(f"Leter etter flaskehalser (Vekt<{VEKT_KRAV}, Lengde<{LENGDE_KRAV})...")

count = 0
cols = ["SHAPE@", ID_FIELD, "STARTPOS", "SLUTTPOS", "TILLATT_TONN", "MAKS_LENGDE", "FRI_HOYDE", "FLASKEHALS", "BEGRENSNING_TYPE", "BESKRIVELSE"]

with arcpy.da.InsertCursor(OUT_FC, cols) as icur:
    with arcpy.da.SearchCursor(IN_FC, read_fields) as scur:
        for row in scur:
            geom = row[0]
            vid = row[1]
            vekt = row[2]
            s0 = row[3]
            s1 = row[4]
            
            # Dynamisk uthenting
            idx = 5
            lengde = row[idx] if has_lengde else None
            if has_lengde: idx += 1
            hoyde = row[idx] if has_hoyde else None
            
            feil_liste = []
            
            # 1. Vekt
            if vekt is not None and vekt < VEKT_KRAV:
                feil_liste.append(f"Vekt ({vekt}t)")
            
            # 2. Lengde
            if lengde is not None and lengde < LENGDE_KRAV:
                feil_liste.append(f"Lengde ({lengde}m)")
            
            # 3. Høyde
            if hoyde is not None and hoyde < HOYDE_KRAV:
                feil_liste.append(f"Høyde ({hoyde}m)")
            
            if not feil_liste: continue
            
            beskrivelse = ", ".join(feil_liste)
            
            # --- NY LOGIKK FOR KORT BESKRIVELSE ---
            typer = []
            if "Vekt" in beskrivelse: typer.append("Vekt")
            if "Lengde" in beskrivelse: typer.append("Lengde")
            if "Høyde" in beskrivelse: typer.append("Høyde")
            
            begrensning = " og ".join(typer) if typer else "Annet"
            
            icur.insertRow((geom, vid, s0, s1, vekt, lengde, hoyde, "JA", begrensning, beskrivelse))
            count += 1

print(f"✅ Ferdig! Fant {count} flaskehals-segmenter.")
