# 02_bygg_tillat_profil_FIXED.py
# ✅ FIXED: Kobler Høyde (hele lenken) + Lengde + Vekt

import arcpy
import os

arcpy.env.overwriteOutput = True

# OPPDATER DETTE HVIS GDB-NAVNET ER ANNERLEDES HOS DEG!
GDB = r"D:\Conda\Flaskehalser\gdb\nvdb_radata.gdb"

# Input Feature Classes
VEG_FC = os.path.join(GDB, "Vegnett")
BK_FC = os.path.join(GDB, "Bruksklasse")
BRU_FC = os.path.join(GDB, "Bruer")
HOYDE_FC = os.path.join(GDB, "Hoydebegrensning_591") 

OUT_FC = os.path.join(GDB, "Veg_TillatProfil")

ID = "VEGLENKESEKV_ID"
EPS = 1e-6

# Hjelpefunksjon for å laste data til dictionary
def load_data(fc, fields, key_idx=0):
    data = {}
    if not arcpy.Exists(fc):
        print(f"⚠️  ADVARSEL: Finner ikke {fc}")
        return {}
        
    # Sjekk om alle felter finnes
    valid_fields = [f.name for f in arcpy.ListFields(fc)]
    read_fields = [f for f in fields if f in valid_fields]
    
    if len(read_fields) < len(fields):
        missing = set(fields) - set(read_fields)
        print(f"⚠️  ADVARSEL: Mangler felter i {os.path.basename(fc)}: {missing}")
    
    with arcpy.da.SearchCursor(fc, read_fields) as cur:
        for row in cur:
            key = row[key_idx]
            # Lagre hele raden
            if key not in data: data[key] = []
            data[key].append(row)
    return data

print("Laster referansedata...")

# 1. Bruksklasse (Vekt + Lengde)
# Vi forventer: ID, START, SLUTT, BK_VERDI, MAKS_LENGDE
bk_fields_req = [ID, "STARTPOS", "SLUTTPOS", "BK_VERDI", "MAKS_LENGDE"]
bk_data = load_data(BK_FC, bk_fields_req)

# 2. Bruer (Vekt)
# Vi forventer: ID, START, SLUTT, TILLATT_TONN, BRU_NAVN
bru_fields_req = [ID, "STARTPOS", "SLUTTPOS", "TILLATT_TONN", "BRU_NAVN"]
bru_data = load_data(BRU_FC, bru_fields_req)

# 3. Høydebegrensning (Høyde)
# Vi forventer: VEGLENKESEKV_ID, SKILTET_HOYDE
hoyde_fields_req = [ID, "SKILTET_HOYDE"]
hoyde_data = load_data(HOYDE_FC, hoyde_fields_req)

print(f"Oppretter {OUT_FC}...")
if arcpy.Exists(OUT_FC): arcpy.management.Delete(OUT_FC)
arcpy.management.CopyFeatures(VEG_FC, OUT_FC)

# Definer nye felter
new_fields = [
    ("BK_VERDI", "LONG"),
    ("MIN_BRU_TONN", "LONG"),
    ("BRU_NAVN", "TEXT", 100),
    ("MAKS_LENGDE", "DOUBLE"),    # Nytt
    ("MIN_HOYDE", "DOUBLE"),      # Nytt
    ("TILLATT_TONN", "LONG"),     # Sluttresultat Vekt
    ("BEGRENSNING_KILDE", "TEXT", 50)
]

for f in new_fields:
    arcpy.management.AddField(OUT_FC, f[0], f[1], field_length=f[2] if len(f)>2 else None)

print("Kalkulerer profil...")

cols = [
    ID, "STARTPOS", "SLUTTPOS", 
    "BK_VERDI", "MIN_BRU_TONN", "BRU_NAVN", 
    "MAKS_LENGDE", "MIN_HOYDE", 
    "TILLATT_TONN", "BEGRENSNING_KILDE"
]

updates = 0
with arcpy.da.UpdateCursor(OUT_FC, cols) as cur:
    for row in cur:
        vid, v0, v1 = row[0], row[1], row[2]
        
        # --- 1. Finn BK (Vekt + Lengde) ---
        curr_bk = 999
        curr_len = 999.0
        
        if vid in bk_data:
            # bk_data struktur: (ID, START, SLUTT, BK_VERDI, MAKS_LENGDE)
            # Merk: Indekser avhenger av load_data rekkefølge. 
            # Vi antar load_data beholder rekkefølgen i 'read_fields'.
            # Men for sikkerhets skyld, la oss finne riktig index dynamisk eller bare anta standard.
            # Med load_data slik den er nå:
            # rad[0]=ID, rad[1]=START, rad[2]=SLUTT, rad[3]=BK, rad[4]=LENGDE (hvis den finnes)
            
            for b_row in bk_data[vid]:
                b_start, b_slutt = b_row[1], b_row[2]
                
                # Overlapp sjekk
                if max(v0, b_start) < min(v1, b_slutt):
                    # Vekt
                    val = b_row[3]
                    if val is not None:
                        curr_bk = min(curr_bk, val)
                    
                    # Lengde (index 4 hvis den ble lastet)
                    if len(b_row) > 4:
                        l_val = b_row[4]
                        if l_val is not None:
                            curr_len = min(curr_len, l_val)

        # --- 2. Finn Bru (Vekt) ---
        curr_bru = 999
        curr_bru_navn = None
        
        if vid in bru_data:
            for b_row in bru_data[vid]:
                b_start, b_slutt = b_row[1], b_row[2]
                
                if max(v0, b_start) < min(v1, b_slutt):
                    val = b_row[3]
                    if val is not None:
                        if val < curr_bru:
                            curr_bru = val
                            curr_bru_navn = b_row[4] # Navn er index 4

        # --- 3. Finn Høyde (Gjelder HELE lenken) ---
        curr_hoy = 999.0
        
        if vid in hoyde_data:
            for h_row in hoyde_data[vid]:
                # h_row: (ID, SKILTET_HOYDE)
                h_val = h_row[1]
                if h_val is not None:
                    curr_hoy = min(curr_hoy, h_val)

        # --- Sammenstill Resultater ---
        
        # Vekt
        res_bk = curr_bk if curr_bk != 999 else None
        res_bru = curr_bru if curr_bru != 999 else None
        
        limit_vekt = min(curr_bk, curr_bru)
        if limit_vekt == 999: limit_vekt = None
        
        # Lengde
        res_len = curr_len if curr_len != 999.0 else None
        
        # Høyde
        res_hoy = curr_hoy if curr_hoy != 999.0 else None
        
        # Kilde
        kilde = "UKJENT"
        if curr_bru < curr_bk:
            kilde = "BRU"
        elif curr_bk < 999:
            kilde = "VEGLISTE"
            
        # Skriv til rad
        row[3] = res_bk         # BK_VERDI
        row[4] = res_bru        # MIN_BRU_TONN
        row[5] = curr_bru_navn  # BRU_NAVN
        row[6] = res_len        # MAKS_LENGDE
        row[7] = res_hoy        # MIN_HOYDE
        row[8] = limit_vekt     # TILLATT_TONN
        row[9] = kilde          # BEGRENSNING_KILDE
        
        cur.updateRow(row)
        updates += 1

print(f"✅ Ferdig! Oppdaterte {updates} segmenter med Vekt, Lengde og Høyde.")
