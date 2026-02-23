# ---------------------------------------------------------
# 05_klassifiser_aarsak_v4.py
#
# FORMELL_BEGR:
#   Stedfestet begrensning (NVDB-korrekt)
#
# OPPGRADERINGSFLASKEHALS:
#   Hva må bygges for å nå ønsket tonnasje
#
# KORRIDOR_BEGR:
#   Hva som stopper gjennomkjøring i praksis
#   (én svak bru stopper hele veglenka)
# ---------------------------------------------------------

import arcpy
import os

arcpy.env.overwriteOutput = True

# -----------------------------
GDB = r"G:\Test\Prosjekt_2025\FlaskerUtenhals\gdb\nvdb_radata.gdb"

FLASKE_FC = os.path.join(GDB, "Flaskehalser_BK900")
PROFIL_FC = os.path.join(GDB, "Veg_TillatProfil")
OUT_FC    = os.path.join(GDB, "Flaskehalser_BK900_V4")

ID_FIELD = "VEGLENKESEKV_ID"

VEKT_KRAV   = 50.0
LENGDE_KRAV = 24.0
HOYDE_KRAV  = 4.5

EPS = 1e-6

# -----------------------------
def overlap(a0, a1, b0, b1):
    return max(a0, b0) <= min(a1, b1) + EPS

def min_or_none(vals):
    v = [x for x in vals if x is not None]
    return min(v) if v else None

# -----------------------------
print("Kopierer flaskehalser...")
if arcpy.Exists(OUT_FC):
    arcpy.management.Delete(OUT_FC)

arcpy.management.CopyFeatures(FLASKE_FC, OUT_FC)

# -----------------------------
# FELTER
# -----------------------------
existing = {f.name for f in arcpy.ListFields(OUT_FC)}

fields_needed = [
    ("FORMELL_BEGR", "TEXT", 20),
    ("OPPGRADERINGSFLASKEHALS", "TEXT", 20),
    ("KORRIDOR_BEGR", "TEXT", 20),
    ("VEG_BK_VERDI", "LONG", None),
    ("BRU_TONN_VERDI", "LONG", None),
    ("MAKS_LENGDE_VERDI", "DOUBLE", None),
    ("FRI_HOYDE_VERDI", "DOUBLE", None),
]

for name, ftype, flen in fields_needed:
    if name not in existing:
        arcpy.management.AddField(OUT_FC, name, ftype, field_length=flen)

# -----------------------------
# PROFILOPPSLAG
# -----------------------------
print("Bygger profilindeks...")

pfields = {f.name for f in arcpy.ListFields(PROFIL_FC)}

read = [
    ID_FIELD, "STARTPOS", "SLUTTPOS",
    "BK_VERDI", "MIN_BRU_TONN"
]

if "MAKS_LENGDE" in pfields:
    read.append("MAKS_LENGDE")
if "MIN_HOYDE" in pfields:
    read.append("MIN_HOYDE")

profil = {}

with arcpy.da.SearchCursor(PROFIL_FC, read) as cur:
    for row in cur:
        vid = int(row[0])
        s0  = float(row[1]) if row[1] else 0.0
        s1  = float(row[2]) if row[2] else 1.0

        bk  = row[3]
        bru = row[4]

        idx = 5
        lng = row[idx] if "MAKS_LENGDE" in pfields else None
        if "MAKS_LENGDE" in pfields:
            idx += 1
        hoy = row[idx] if "MIN_HOYDE" in pfields else None

        profil.setdefault(vid, []).append((s0, s1, bk, bru, lng, hoy))

# -------------------------------------------------
# PASS 1 – stedfestet vurdering
# -------------------------------------------------
print("Beregner stedfestet begrensning...")

bru_korridor = set()

cols = [
    ID_FIELD, "STARTPOS", "SLUTTPOS",
    "FORMELL_BEGR", "OPPGRADERINGSFLASKEHALS",
    "VEG_BK_VERDI", "BRU_TONN_VERDI",
    "MAKS_LENGDE_VERDI", "FRI_HOYDE_VERDI"
]

with arcpy.da.UpdateCursor(OUT_FC, cols) as cur:
    for row in cur:

        vid = int(row[0])
        s0  = float(row[1])
        s1  = float(row[2])

        treff = []
        for p0, p1, bk, bru, lng, hoy in profil.get(vid, []):
            if overlap(s0, s1, p0, p1):
                treff.append((bk, bru, lng, hoy))

        if not treff:
            continue

        veg_bk   = min_or_none([t[0] for t in treff])
        bru_tonn = min_or_none([t[1] for t in treff])
        maks_len = min_or_none([t[2] for t in treff])
        fri_hoy  = min_or_none([t[3] for t in treff])

        # ---- FORMELL ----
        if veg_bk is not None and bru_tonn is not None:
            formell = "BRU" if bru_tonn <= veg_bk else "VEG"
        elif bru_tonn is not None:
            formell = "BRU"
        elif veg_bk is not None:
            formell = "VEG"
        else:
            formell = "UKJENT"

        # ---- OPPGRADERING ----
        veg_under = veg_bk is not None and veg_bk < VEKT_KRAV
        bru_under = bru_tonn is not None and bru_tonn < VEKT_KRAV

        if veg_under and bru_under:
            opp = "BEGGE"
        elif veg_under:
            opp = "VEG"
        elif bru_under:
            opp = "BRU"
        else:
            opp = "OK"

        # registrer bru for korridor
        if bru_tonn is not None and bru_tonn < VEKT_KRAV:
            bru_korridor.add(vid)

        row[3] = formell
        row[4] = opp
        row[5] = veg_bk
        row[6] = bru_tonn
        row[7] = maks_len
        row[8] = fri_hoy

        cur.updateRow(row)

# -------------------------------------------------
# PASS 2 – korridorpropagering
# -------------------------------------------------
print("Propagerer brubegrensning til hele veglenka...")

with arcpy.da.UpdateCursor(OUT_FC, [ID_FIELD, "KORRIDOR_BEGR"]) as cur:
    for vid, korr in cur:
        if int(vid) in bru_korridor:
            korr = "BRU"
        else:
            korr = "VEG"
        cur.updateRow((vid, korr))

print("✅ Ferdig – 05-v4 korridoranalyse fullført.")
