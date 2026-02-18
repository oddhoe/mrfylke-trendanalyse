# -*- coding: utf-8 -*-
"""
05_klassifiser_aarsak_v904_50t_19_5m_4_5m.py

Klassifiserer flaskehalser fra 04 med årsak basert på dimensjonerende verdi:
- Hvis bru < veg  => "BRU"
- Hvis veg < bru  => "VEG"
- Hvis veg == bru => "BRU, VEG"
+ evt. "LENGDE" og/eller "HØYDE" hvis under terskel.

Slår opp mot Veg_TillatProfil.
"""

import arcpy
import os

arcpy.env.overwriteOutput = True

GDB = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"
FLASKE_FC = os.path.join(GDB, "Flaskehalser_BK904_Normal_50t_19_5m_4_5m")
PROFIL_FC = os.path.join(GDB, "Veg_TillatProfil")
OUT_FC = os.path.join(GDB, "Flaskehalser_BK904_Normal_50t_19_5m_4_5m_Aarsak")

ID_FIELD = "VEGLENKESEKV_ID"

# KRAV
VEKT_KRAV = 50.0
LENGDE_KRAV = 19.5
HOYDE_KRAV = 4.5

EPS = 1e-9
STRICT_OVERLAP = True

def overlap(a0, a1, b0, b1, strict=True):
    left = max(a0, b0)
    right = min(a1, b1)
    if strict:
        return left < right - EPS
    return left <= right + EPS

def min_or_none(values):
    vals = [v for v in values if v is not None]
    return min(vals) if vals else None

def ensure_fields(fc, fields):
    existing = {f.name for f in arcpy.ListFields(fc)}
    for name, ftype, flen in fields:
        if name not in existing:
            if flen is None:
                arcpy.management.AddField(fc, name, ftype)
            else:
                arcpy.management.AddField(fc, name, ftype, field_length=flen)

print("Kopierer flaskehalser til nytt lag...")
if arcpy.Exists(OUT_FC):
    arcpy.management.Delete(OUT_FC)
arcpy.management.CopyFeatures(FLASKE_FC, OUT_FC)

need = [
    ("AARSAK_DETALJERT", "TEXT", 80),
    ("VEG_BK_VERDI", "LONG", None),
    ("BRU_TONN_VERDI", "LONG", None),
    ("MAKS_LENGDE_VERDI", "DOUBLE", None),
    ("FRI_HOYDE_VERDI", "DOUBLE", None),
]
ensure_fields(OUT_FC, need)

pfields = {f.name for f in arcpy.ListFields(PROFIL_FC)}
P_BK = "BK_VERDI" if "BK_VERDI" in pfields else None
P_BRU = "MIN_BRU_TONN" if "MIN_BRU_TONN" in pfields else None
P_LEN = "MAKS_LENGDE" if "MAKS_LENGDE" in pfields else None
P_HOY = "MIN_HOYDE" if "MIN_HOYDE" in pfields else None

if not any([P_BK, P_BRU, P_LEN, P_HOY]):
    raise RuntimeError("Fant ingen relevante profil-felt (BK_VERDI/MIN_BRU_TONN/MAKS_LENGDE/MIN_HOYDE).")

print("Bygger oppslag (per veglenke) fra profil...")
idx = {}
read = [ID_FIELD, "STARTPOS", "SLUTTPOS"]
if P_BK: read.append(P_BK)
if P_BRU: read.append(P_BRU)
if P_LEN: read.append(P_LEN)
if P_HOY: read.append(P_HOY)

with arcpy.da.SearchCursor(PROFIL_FC, read) as cur:
    for row in cur:
        vls = int(row[0])
        s0 = float(row[1] if row[1] is not None else 0.0)
        s1 = float(row[2] if row[2] is not None else 1.0)
        k = 3
        bk = row[k] if P_BK else None
        k += 1 if P_BK else 0
        bru = row[k] if P_BRU else None
        k += 1 if P_BRU else 0
        lng = row[k] if P_LEN else None
        k += 1 if P_LEN else 0
        hoy = row[k] if P_HOY else None
        idx.setdefault(vls, []).append((s0, s1, bk, bru, lng, hoy))

print("Klassifiserer årsaker...")
out_fields = [
    ID_FIELD, "STARTPOS", "SLUTTPOS",
    "AARSAK_DETALJERT",
    "VEG_BK_VERDI", "BRU_TONN_VERDI",
    "MAKS_LENGDE_VERDI", "FRI_HOYDE_VERDI",
]

with arcpy.da.UpdateCursor(OUT_FC, out_fields) as ucur:
    for vls, s0, s1, aarsak, veg_bk, bru_tonn, maks_len, fri_hoyde in ucur:
        vls = int(vls)
        s0 = float(s0 if s0 is not None else 0.0)
        s1 = float(s1 if s1 is not None else 1.0)

        hits = []
        for p0, p1, bk, bru, lng, hoy in idx.get(vls, []):
            if overlap(s0, s1, p0, p1, strict=STRICT_OVERLAP):
                hits.append((bk, bru, lng, hoy))

        if not hits:
            continue

        veg_bk = min_or_none([h[0] for h in hits])
        bru_tonn = min_or_none([h[1] for h in hits])
        maks_len = min_or_none([h[2] for h in hits])
        fri_hoyde = min_or_none([h[3] for h in hits])

        tags = []

        if veg_bk is not None or bru_tonn is not None:
            dims_val = min([v for v in [veg_bk, bru_tonn] if v is not None])
            if dims_val is not None and float(dims_val) < VEKT_KRAV:
                if veg_bk is not None and bru_tonn is not None and veg_bk == bru_tonn:
                    tags.extend(["BRU", "VEG"])
                elif bru_tonn is not None and (veg_bk is None or bru_tonn < veg_bk):
                    tags.append("BRU")
                else:
                    tags.append("VEG")

        if maks_len is not None and float(maks_len) < LENGDE_KRAV:
            tags.append("LENGDE")

        if fri_hoyde is not None and float(fri_hoyde) < HOYDE_KRAV:
            tags.append("HØYDE")

        aarsak = "OK" if not tags else ", ".join(tags)
        ucur.updateRow((vls, s0, s1, aarsak, veg_bk, bru_tonn, maks_len, fri_hoyde))

print("✅ Ferdig!")
