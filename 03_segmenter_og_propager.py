# 03_korridor_dim_kilde.py
#
# Lager:
# 1) Veg_TillatSegmentert: samme geometri som Veg_TillatProfil, men med propagert min-verdi per VEGLENKESEKV_ID
# 2) Veg_TillatKorridor: Dissolve per VEGLENKESEKV_ID (én linje per id), med DIM_KILDE som "BRU" hvis bru er
#    dimensjonerende hvor som helst på lenka (inkl. likhet BK==BRU)

import arcpy
import os

arcpy.env.overwriteOutput = True

# --- GDB / FC ---
GDB = r"G:\Test\Prosjekt_2025\FlaskerUtenhals\gdb\nvdb_radata.gdb"
IN_FC = os.path.join(GDB, "Veg_TillatProfil")

OUT_SEG_FC = os.path.join(GDB, "Veg_TillatSegmentert")   # segmenter (samme geometri som IN)
OUT_KORR_FC = os.path.join(GDB, "Veg_TillatKorridor")    # dissolve (én linje per ID)

ID_FIELD = "VEGLENKESEKV_ID"

# Felt vi forventer fra steg 02
F_TONN = "TILLATT_TONN"
F_LEN = "MAKS_LENGDE"
F_HOY = "MIN_HOYDE"
F_BK = "BK_VERDI"
F_BRU = "MIN_BRU_TONN"

# Nytt felt i output
F_DIM = "DIM_KILDE"   # "BRU" eller "VEG"
F_PROP = "PROPAGERT"  # "JA"/"NEI" (segment-output)

# --- Hjælp ---
def min_or_none(vals):
    v = [x for x in vals if x is not None]
    return min(v) if v else None

def ensure_field(fc, name, ftype, length=None):
    existing = {f.name for f in arcpy.ListFields(fc)}
    if name not in existing:
        if length is None:
            arcpy.management.AddField(fc, name, ftype)
        else:
            arcpy.management.AddField(fc, name, ftype, field_length=length)

def dims_kilde_for_segment(bk, bru):
    """
    Returnerer "BRU" hvis bru er dimensjonerende (bru < bk, eller bru == bk),
    ellers "VEG" hvis veg er dimensjonerende.
    Hvis bare én finnes: bruk den.
    """
    if bk is None and bru is None:
        return None
    if bk is None and bru is not None:
        return "BRU"
    if bru is None and bk is not None:
        return "VEG"
    # begge finnes:
    if bru <= bk:
        return "BRU"
    return "VEG"

print("Leser stats per veglenke (minverdier + dim-kilde)...")

# Sjekk felt
fields_in = {f.name for f in arcpy.ListFields(IN_FC)}
missing = [f for f in [ID_FIELD, F_TONN, F_BK, F_BRU] if f not in fields_in]
if missing:
    raise RuntimeError(f"Mangler felt i {IN_FC}: {missing}")

has_len = F_LEN in fields_in
has_hoy = F_HOY in fields_in

read_fields = [ID_FIELD, F_TONN, F_BK, F_BRU]
if has_len:
    read_fields.append(F_LEN)
if has_hoy:
    read_fields.append(F_HOY)

# Samle min-verdier og "finnes BRU-dimensjonerende" per ID
stats = {}  # {vid: {'tonn':..., 'len':..., 'hoy':..., 'has_bru_dim': bool, 'has_any': bool}}
with arcpy.da.SearchCursor(IN_FC, read_fields) as cur:
    for row in cur:
        vid = row[0]
        tonn = row[1]
        bk = row[2]
        bru = row[3]
        idx = 4
        lengde = row[idx] if has_len else None
        idx += 1 if has_len else 0
        hoyde = row[idx] if has_hoy else None

        if vid not in stats:
            stats[vid] = {
                "tonn": None,
                "len": None,
                "hoy": None,
                "has_bru_dim": False,
                "has_any": False,
            }

        s = stats[vid]
        s["has_any"] = True

        if tonn is not None:
            s["tonn"] = tonn if s["tonn"] is None else min(s["tonn"], tonn)
        if lengde is not None:
            s["len"] = lengde if s["len"] is None else min(s["len"], lengde)
        if hoyde is not None:
            s["hoy"] = hoyde if s["hoy"] is None else min(s["hoy"], hoyde)

        seg_dim = dims_kilde_for_segment(bk, bru)
        if seg_dim == "BRU":
            s["has_bru_dim"] = True

print(f"Fant {len(stats)} veglenker.")

# ------------------------------------------------------------
# 1) Segment-output: CopyFeatures(IN_FC) + propagerte felt + DIM_KILDE
# ------------------------------------------------------------
print("Oppretter segment-output...")
if arcpy.Exists(OUT_SEG_FC):
    arcpy.management.Delete(OUT_SEG_FC)

arcpy.management.CopyFeatures(IN_FC, OUT_SEG_FC)

# Legg til felt som vi fyller
ensure_field(OUT_SEG_FC, "TONN_PROP", "LONG")
if has_len:
    ensure_field(OUT_SEG_FC, "LEN_PROP", "DOUBLE")
if has_hoy:
    ensure_field(OUT_SEG_FC, "HOY_PROP", "DOUBLE")
ensure_field(OUT_SEG_FC, F_DIM, "TEXT", length=10)
ensure_field(OUT_SEG_FC, F_PROP, "TEXT", length=10)

upd_fields = [ID_FIELD, "TONN_PROP", F_DIM, F_PROP]
if has_len:
    upd_fields.insert(2, "LEN_PROP")  # etter TONN_PROP
if has_hoy:
    # plasser etter LEN_PROP hvis den finnes, ellers etter TONN_PROP
    insert_pos = 3 if has_len else 2
    upd_fields.insert(insert_pos, "HOY_PROP")

with arcpy.da.UpdateCursor(OUT_SEG_FC, upd_fields) as ucur:
    for row in ucur:
        vid = row[0]
        s = stats.get(vid)

        if not s or not s["has_any"]:
            # Noe er rart, men la stå
            row[-1] = "NEI"
            ucur.updateRow(row)
            continue

        # Finn posisjoner dynamisk
        # row: [vid, TONN_PROP, (LEN_PROP), (HOY_PROP), DIM_KILDE, PROPAGERT]
        row[1] = s["tonn"]
        # LEN_PROP / HOY_PROP hvis de finnes
        offset = 2
        if has_len:
            row[offset] = s["len"]
            offset += 1
        if has_hoy:
            row[offset] = s["hoy"]
            offset += 1

        # DIM_KILDE for hele lenka
        row[offset] = "BRU" if s["has_bru_dim"] else "VEG"
        row[offset + 1] = "JA"

        ucur.updateRow(row)

print("✅ Ferdig segment-output.")

# ------------------------------------------------------------
# 2) Korridor-output: Dissolve per ID + DIM_KILDE
# ------------------------------------------------------------
print("Oppretter korridor-output (dissolve per VEGLENKESEKV_ID)...")
if arcpy.Exists(OUT_KORR_FC):
    arcpy.management.Delete(OUT_KORR_FC)

# Dissolve-statistikk: min tonn + min lengde + min høyde
stat_fields = [[F_TONN, "MIN"]]
if has_len:
    stat_fields.append([F_LEN, "MIN"])
if has_hoy:
    stat_fields.append([F_HOY, "MIN"])

arcpy.management.Dissolve(
    in_features=IN_FC,
    out_feature_class=OUT_KORR_FC,
    dissolve_field=ID_FIELD,
    statistics_fields=stat_fields,
    multi_part="MULTI_PART",
    unsplit_lines="DISSOLVE_LINES"
)

# Legg til DIM_KILDE
ensure_field(OUT_KORR_FC, F_DIM, "TEXT", length=10)

# Oppdater DIM_KILDE basert på dict
with arcpy.da.UpdateCursor(OUT_KORR_FC, [ID_FIELD, F_DIM]) as ucur:
    for vid, dim in ucur:
        s = stats.get(vid)
        dim_val = "BRU" if (s and s["has_bru_dim"]) else "VEG"
        ucur.updateRow((vid, dim_val))

print("✅ Ferdig! Nå kan du symbolisere hele korridoren på DIM_KILDE = 'BRU'.")
