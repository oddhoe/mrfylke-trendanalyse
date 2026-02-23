# 05_klassifiser_aarsak_FIXED.py
# ✅ FIXED: Bru-prioritering og robust overlap

import arcpy
import os

arcpy.env.overwriteOutput = True

GDB = r"G:\Test\Prosjekt_2025\FlaskerUtenhals\gdb\nvdb_radata.gdb"
FLASKE_FC = os.path.join(GDB, "Flaskehalser_BK900")
PROFIL_FC = os.path.join(GDB, "Veg_TillatProfil")
OUT_FC = os.path.join(GDB, "Flaskehalser_BK900_Aarsak")

ID_FIELD = "VEGLENKESEKV_ID"
EPS = 1e-6
VEKT_KRAV = 50
LENGDE_KRAV = 24.0
HOYDE_KRAV = 4.5

def overlap(a0, a1, b0, b1):
    return max(a0, b0) <= min(a1, b1) + EPS

def min_or_none(values):
    vals = [v for v in values if v is not None]
    return min(vals) if vals else None

print("Kopierer til nytt lag...")
if arcpy.Exists(OUT_FC): arcpy.management.Delete(OUT_FC)
arcpy.management.CopyFeatures(FLASKE_FC, OUT_FC)

if "TILLATT_TONN" in [f.name for f in arcpy.ListFields(OUT_FC)]:
    with arcpy.da.UpdateCursor(OUT_FC, ["TILLATT_TONN"]) as cur:
        for (t,) in cur:
            if t is None: cur.deleteRow()

existing = {f.name for f in arcpy.ListFields(OUT_FC)}
need = [("AARSAK_DETALJERT", "TEXT", 50), ("VEG_BK_VERDI", "LONG", None), ("BRU_TONN_VERDI", "LONG", None), ("MAKS_LENGDE_VERDI", "DOUBLE", None), ("FRI_HOYDE_VERDI", "DOUBLE", None)]

for name, ftype, flen in need:
    if name not in existing: arcpy.management.AddField(OUT_FC, name, ftype, field_length=flen)

print("Bygger oppslag fra profil...")
pfields = {f.name for f in arcpy.ListFields(PROFIL_FC)}
P_BK = "BK_VERDI" if "BK_VERDI" in pfields else None
P_BRU = "MIN_BRU_TONN" if "MIN_BRU_TONN" in pfields else None
P_LEN = "MAKS_LENGDE" if "MAKS_LENGDE" in pfields else None
P_HOY = "MIN_HOYDE" if "MIN_HOYDE" in pfields else None

idx = {}
read = [ID_FIELD, "STARTPOS", "SLUTTPOS", P_BK, P_BRU]
if P_LEN: read.append(P_LEN)
if P_HOY: read.append(P_HOY)

with arcpy.da.SearchCursor(PROFIL_FC, read) as cur:
    for row in cur:
        vls = int(row[0])
        s0 = float(row[1]) if row[1] else 0.0
        s1 = float(row[2]) if row[2] else 1.0
        bk = row[3]
        bru = row[4]
        lng = row[5] if P_LEN and P_HOY else (row[5] if P_LEN else None)
        hoy = row[6] if (P_LEN and P_HOY) else (row[5] if (P_HOY and not P_LEN) else None)
        idx.setdefault(vls, []).append((s0, s1, bk, bru, lng, hoy))

print("Klassifiserer årsaker...")
out_fields = [ID_FIELD, "STARTPOS", "SLUTTPOS", "AARSAK_DETALJERT", "VEG_BK_VERDI", "BRU_TONN_VERDI", "MAKS_LENGDE_VERDI", "FRI_HOYDE_VERDI"]

with arcpy.da.UpdateCursor(OUT_FC, out_fields) as ucur:
    for vls, s0, s1, aarsak, veg_bk, bru_tonn, maks_len, fri_hoyde in ucur:
        vls = int(vls)
        s0 = float(s0) if s0 else 0.0
        s1 = float(s1) if s1 else 1.0
        
        hits = []
        for p0, p1, bk, bru, lng, hoy in idx.get(vls, []):
            if overlap(s0, s1, p0, p1):
                hits.append((bk, bru, lng, hoy))
        
        if not hits: continue
        
        veg_bk = min_or_none([h[0] for h in hits])
        bru_tonn = min_or_none([h[1] for h in hits])
        maks_len = min_or_none([h[2] for h in hits])
        fri_hoyde = min_or_none([h[3] for h in hits])
        
        tags = []
        # BRU PRIORITERING
        if bru_tonn is not None and bru_tonn < VEKT_KRAV: tags.append("BRU")
        elif veg_bk is not None and veg_bk < VEKT_KRAV: tags.append("VEG")
        
        if maks_len is not None and maks_len < LENGDE_KRAV: tags.append("LENGDE")
        if fri_hoyde is not None and fri_hoyde < HOYDE_KRAV: tags.append("HØYDE")
        
        aarsak = "OK" if not tags else ", ".join(tags)
        ucur.updateRow((vls, s0, s1, aarsak, veg_bk, bru_tonn, maks_len, fri_hoyde))

print("✅ Ferdig!")
