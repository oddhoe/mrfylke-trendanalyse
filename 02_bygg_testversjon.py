# 02_bygg_tillat_profil_KORRIDOR.py
#
# Bygger Veg_TillatProfil med vekt/lengde/høyde (som original 02),
# + korrigering for "hele korridor":
#   - DIM_KILDE = "BRU" for hele VEGLENKESEKV_ID dersom bru er dimensjonerende noe sted
#     (min_bru <= min_bk, og likhet tolkes som bru-årsak).
#
# Segmentfelt (per rad/segment):
#   BK_VERDI, MAKS_LENGDE, MIN_BRU_TONN, BRU_NAVN, MIN_HOYDE, TILLATT_TONN, BEGRENSNING_KILDE
#
# Korridorfelt (propagert likt på alle segmenter med samme VEGLENKESEKV_ID):
#   DIM_KILDE

import arcpy
import os

arcpy.env.overwriteOutput = True

# --- OPPDATER DETTE HVIS GDB-NAVNET ER ANNERLEDES HOS DEG ---
GDB = r"G:\Test\Prosjekt_2025\FlaskerUtenhals\gdb\nvdb_radata.gdb"

# Input Feature Classes
VEG_FC = os.path.join(GDB, "Vegnett")
BK_FC = os.path.join(GDB, "Bruksklasse")
BRU_FC = os.path.join(GDB, "Bruer")
HOYDE_FC = os.path.join(GDB, "Hoydebegrensning_591")

# Output Feature Class
OUT_FC = os.path.join(GDB, "Veg_TillatProfil")

ID = "VEGLENKESEKV_ID"


def load_data(fc, fields, key_idx=0):
    data = {}
    if not arcpy.Exists(fc):
        print(f"⚠️ ADVARSEL: Finner ikke {fc}")
        return {}

    valid_fields = [f.name for f in arcpy.ListFields(fc)]
    read_fields = [f for f in fields if f in valid_fields]
    if len(read_fields) < len(fields):
        missing = set(fields) - set(read_fields)
        print(f"⚠️ ADVARSEL: Mangler felter i {os.path.basename(fc)}: {missing}")

    with arcpy.da.SearchCursor(fc, read_fields) as cur:
        for row in cur:
            key = row[key_idx]
            data.setdefault(key, []).append(row)

    return data


def overlap_pos(a0, a1, b0, b1):
    # Streng overlapp (positiv lengde)
    return max(a0, b0) < min(a1, b1)


def ensure_field(fc, name, ftype, length=None):
    existing = {f.name for f in arcpy.ListFields(fc)}
    if name not in existing:
        if length is None:
            arcpy.management.AddField(fc, name, ftype)
        else:
            arcpy.management.AddField(fc, name, ftype, field_length=length)


def corridor_dim_kilde(min_bk, min_bru):
    # BRU hvis bru er dimensjonerende, inkl likhet (<=)
    if min_bru is not None and (min_bk is None or min_bru <= min_bk):
        return "BRU"
    if min_bk is not None:
        return "VEG"
    return None


print("Laster referansedata...")

# 1) Bruksklasse (Vekt + Lengde)
bk_fields_req = [ID, "STARTPOS", "SLUTTPOS", "BK_VERDI", "MAKS_LENGDE"]
bk_data = load_data(BK_FC, bk_fields_req)

# 2) Bruer (Vekt)
bru_fields_req = [ID, "STARTPOS", "SLUTTPOS", "TILLATT_TONN", "BRU_NAVN"]
bru_data = load_data(BRU_FC, bru_fields_req)

# 3) Høydebegrensning (Høyde) - gjelder hele lenken
hoyde_fields_req = [ID, "SKILTET_HOYDE"]
hoyde_data = load_data(HOYDE_FC, hoyde_fields_req)

# --- Precompute per ID (korridor) minverdier for DIM_KILDE ---
print("Beregner korridor-minverdier (per VEGLENKESEKV_ID)...")
id_min_bk = {}
id_min_bru = {}

# BK min per ID (tar min av alle BK_VERDI-rader på ID)
for vid, rows in bk_data.items():
    m = None
    for r in rows:
        val = r[3]  # BK_VERDI
        if val is None:
            continue
        m = val if m is None else min(m, val)
    id_min_bk[vid] = m

# BRU min per ID (tar min av alle TILLATT_TONN-rader på ID)
for vid, rows in bru_data.items():
    m = None
    for r in rows:
        val = r[3]  # TILLATT_TONN
        if val is None:
            continue
        m = val if m is None else min(m, val)
    id_min_bru[vid] = m

print(f"Oppretter {OUT_FC}...")
if arcpy.Exists(OUT_FC):
    arcpy.management.Delete(OUT_FC)

arcpy.management.CopyFeatures(VEG_FC, OUT_FC)

# Definer nye felt (som original + DIM_KILDE)
ensure_field(OUT_FC, "BK_VERDI", "LONG")
ensure_field(OUT_FC, "MIN_BRU_TONN", "LONG")
ensure_field(OUT_FC, "BRU_NAVN", "TEXT", length=100)
ensure_field(OUT_FC, "MAKS_LENGDE", "DOUBLE")
ensure_field(OUT_FC, "MIN_HOYDE", "DOUBLE")
ensure_field(OUT_FC, "TILLATT_TONN", "LONG")
ensure_field(OUT_FC, "BEGRENSNING_KILDE", "TEXT", length=50)
ensure_field(OUT_FC, "DIM_KILDE", "TEXT", length=10)  # propagert for hele korridoren

print("Kalkulerer profil (segment) + DIM_KILDE (korridor)...")

cols = [
    ID, "STARTPOS", "SLUTTPOS",
    "BK_VERDI", "MIN_BRU_TONN", "BRU_NAVN",
    "MAKS_LENGDE", "MIN_HOYDE",
    "TILLATT_TONN", "BEGRENSNING_KILDE",
    "DIM_KILDE",
]

updates = 0

with arcpy.da.UpdateCursor(OUT_FC, cols) as cur:
    for row in cur:
        vid = row[0]
        v0 = float(row[1]) if row[1] is not None else 0.0
        v1 = float(row[2]) if row[2] is not None else 1.0

        # --- 1) Segment: min BK + min lengde innenfor segmentets overlapp ---
        curr_bk = 999
        curr_len = 999.0

        if vid in bk_data:
            for b_row in bk_data[vid]:
                b_start, b_slutt = b_row[1], b_row[2]
                if b_start is None or b_slutt is None:
                    continue
                if overlap_pos(v0, v1, float(b_start), float(b_slutt)):
                    bk_val = b_row[3]
                    if bk_val is not None:
                        curr_bk = min(curr_bk, bk_val)

                    # MAKS_LENGDE
                    if len(b_row) > 4:
                        l_val = b_row[4]
                        if l_val is not None:
                            curr_len = min(curr_len, l_val)

        # --- 2) Segment: min bru-tonn innenfor segmentets overlapp ---
        curr_bru = 999
        curr_bru_navn = None

        if vid in bru_data:
            for b_row in bru_data[vid]:
                b_start, b_slutt = b_row[1], b_row[2]
                if b_start is None or b_slutt is None:
                    continue
                if overlap_pos(v0, v1, float(b_start), float(b_slutt)):
                    bru_val = b_row[3]
                    if bru_val is not None and bru_val < curr_bru:
                        curr_bru = bru_val
                        curr_bru_navn = b_row[4]

        # --- 3) Segment: min høyde (gjelder hele lenken) ---
        curr_hoy = 999.0
        if vid in hoyde_data:
            for h_row in hoyde_data[vid]:
                h_val = h_row[1]
                if h_val is not None:
                    curr_hoy = min(curr_hoy, h_val)

        # --- 4) Segment: sammenstill ---
        res_bk = curr_bk if curr_bk != 999 else None
        res_bru = curr_bru if curr_bru != 999 else None
        res_len = curr_len if curr_len != 999.0 else None
        res_hoy = curr_hoy if curr_hoy != 999.0 else None

        # Sluttresultat vekt (segment)
        limit_vekt = min(curr_bk, curr_bru)
        if limit_vekt == 999:
            limit_vekt = None

        # --- 5) Segment: kilde (justert til <= for "bru sannsynlig årsak ved likhet") ---
        kilde = "UKJENT"
        if res_bru is not None and (res_bk is None or res_bru <= res_bk):
            kilde = "BRU"
        elif res_bk is not None:
            kilde = "VEGLISTE"

        # --- 6) Korridor: DIM_KILDE (samme på alle segmenter av samme ID) ---
        dim = corridor_dim_kilde(id_min_bk.get(vid), id_min_bru.get(vid))

        # --- 7) Skriv til rad ---
        row[3] = res_bk
        row[4] = res_bru
        row[5] = curr_bru_navn
        row[6] = res_len
        row[7] = res_hoy
        row[8] = limit_vekt
        row[9] = kilde
        row[10] = dim

        cur.updateRow(row)
        updates += 1

print(f"✅ Ferdig! Oppdaterte {updates} segmenter i Veg_TillatProfil.")
print("Tips: Symboliser på DIM_KILDE for å få hele korridoren til å vise BRU/VEG likt.")