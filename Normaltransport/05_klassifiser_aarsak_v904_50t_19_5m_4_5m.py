# -*- coding: utf-8 -*-
"""
05_klassifiser_aarsak_v904_50t_19_5m_4_5m.py

Klassifiserer flaskehalser fra 04 med årsak basert på propagerte verdier.

Leser fra Veg_TillatSegmentert (propagerte min-verdier per VEGLENKESEKV_ID)
slik at f.eks. bru lenger opp på lenka riktig klassifiseres som årsak.

Årsak-logikk:
  - TONN_PROP < 50t og DIM_KILDE = BRU  → "BRU"
  - TONN_PROP < 50t og DIM_KILDE = VEG  → "VEG"
  - TONN_PROP < 50t og begge like        → "BRU, VEG"
  - MIN_BRU_TONN 50–59t (selvstendig)    → "BRU60"
  - LEN_PROP < 19.5m                     → "LENGDE"
  - HOY_PROP < 4.5m                      → "HØYDE"

Fallback til Veg_TillatProfil hvis Veg_TillatSegmentert ikke finnes.
"""

import arcpy
import os

arcpy.env.overwriteOutput = True

# ------------------------------
# KONFIG
# ------------------------------
GDB      = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"
FLASKE_FC = os.path.join(GDB, "Flaskehalser_BK904_Normal_50t_19_5m_4_5m")
OUT_FC    = os.path.join(GDB, "Flaskehalser_BK904_Normal_50t_19_5m_4_5m_Aarsak")
ID_FIELD  = "VEGLENKESEKV_ID"

# Foretrekk propagert; fallback til råprofil
_SEG  = os.path.join(GDB, "Veg_TillatSegmentert")
_PROF = os.path.join(GDB, "Veg_TillatProfil")
if arcpy.Exists(_SEG):
    PROFIL_FC = _SEG
    print("Leser fra: Veg_TillatSegmentert (propagerte verdier) ✅")
elif arcpy.Exists(_PROF):
    PROFIL_FC = _PROF
    print("⚠️  Veg_TillatSegmentert mangler — leser fra Veg_TillatProfil (ikke-propagert).")
else:
    raise FileNotFoundError(f"Verken Veg_TillatSegmentert eller Veg_TillatProfil finnes i {GDB}.")

# --- KRAV ---
VEKT_KRAV     = 50.0
BRU_TONN_KRAV = 60.0   # bru 50–59t er selvstendig begrensning (BRU60)
LENGDE_KRAV   = 19.5
HOYDE_KRAV    = 4.5
EPS           = 1e-9
STRICT_OVERLAP = True


# ------------------------------
# HJELPEFUNKSJONER
# ------------------------------
def overlap(a0, a1, b0, b1, strict=True):
    left  = max(a0, b0)
    right = min(a1, b1)
    return (left < right - EPS) if strict else (left <= right + EPS)


def min_or_none(values):
    vals = [v for v in values if v is not None]
    return min(vals) if vals else None


def ensure_fields(fc, fields):
    existing = {f.name for f in arcpy.ListFields(fc)}
    for fname, ftype, flen in fields:
        if fname not in existing:
            if flen is None:
                arcpy.management.AddField(fc, fname, ftype)
            else:
                arcpy.management.AddField(fc, fname, ftype, field_length=flen)


# ------------------------------
# KOPIER FLASKEHALSER TIL OUTPUT
# ------------------------------
print("Kopierer flaskehalser til nytt lag...")
if arcpy.Exists(OUT_FC):
    arcpy.management.Delete(OUT_FC)
arcpy.management.CopyFeatures(FLASKE_FC, OUT_FC)

ensure_fields(OUT_FC, [
    ("AARSAK_DETALJERT",  "TEXT",   100),
    ("TONN_PROP_VERDI",   "LONG",   None),   # propagert dimensjonerende tonn
    ("VEG_BK_VERDI",      "LONG",   None),   # BK-verdi fra vegnettet
    ("BRU_TONN_VERDI",    "LONG",   None),   # min bru-tonn på lenka
    ("MAKS_LENGDE_VERDI", "DOUBLE", None),   # propagert min lengde
    ("FRI_HOYDE_VERDI",   "DOUBLE", None),   # propagert min høyde
    ("DIM_KILDE",         "TEXT",   10),     # BRU / VEG
])


# ------------------------------
# FELTDETEKSJON I PROFIL/SEGMENTERT
# ------------------------------
pfields = {f.name for f in arcpy.ListFields(PROFIL_FC)}

# Propagerte felt foretrekkes, fallback til råfelt
P_TONN = "TONN_PROP"    if "TONN_PROP"    in pfields else "TILLATT_TONN"
P_BK   = "BK_VERDI"     if "BK_VERDI"     in pfields else None
P_BRU  = "MIN_BRU_TONN" if "MIN_BRU_TONN" in pfields else None
P_LEN  = "LEN_PROP"     if "LEN_PROP"     in pfields else ("MAKS_LENGDE" if "MAKS_LENGDE" in pfields else None)
P_HOY  = "HOY_PROP"     if "HOY_PROP"     in pfields else ("MIN_HOYDE"   if "MIN_HOYDE"   in pfields else None)
P_DIM  = "DIM_KILDE"    if "DIM_KILDE"    in pfields else None

print(f"  Tonn-felt  : {P_TONN}")
print(f"  BK-felt    : {P_BK or '(mangler)'}")
print(f"  Bru-felt   : {P_BRU or '(mangler)'}")
print(f"  Lengde-felt: {P_LEN or '(mangler)'}")
print(f"  Høyde-felt : {P_HOY or '(mangler)'}")
print(f"  DIM_KILDE  : {P_DIM or '(mangler — beregnes fra BK+BRU)'}")


# ------------------------------
# BYGG OPPSLAG FRA PROFIL/SEGMENTERT
# ------------------------------
print(f"Bygger oppslag per {ID_FIELD} fra {os.path.basename(PROFIL_FC)}...")

idx = {}   # vid → list[(s0, s1, tonn, bk, bru, lng, hoy, dim)]

read = [ID_FIELD, "STARTPOS", "SLUTTPOS", P_TONN]
if P_BK:  read.append(P_BK)
if P_BRU: read.append(P_BRU)
if P_LEN: read.append(P_LEN)
if P_HOY: read.append(P_HOY)
if P_DIM: read.append(P_DIM)

with arcpy.da.SearchCursor(PROFIL_FC, read) as cur:
    for row in cur:
        vls  = int(row[0])
        s0   = float(row[1] or 0.0)
        s1   = float(row[2] or 1.0)
        k    = 3
        tonn = row[k];                              k += 1
        bk   = row[k] if P_BK  else None;          k += 1 if P_BK  else 0
        bru  = row[k] if P_BRU else None;          k += 1 if P_BRU else 0
        lng  = row[k] if P_LEN else None;          k += 1 if P_LEN else 0
        hoy  = row[k] if P_HOY else None;          k += 1 if P_HOY else 0
        dim  = row[k] if P_DIM else None
        idx.setdefault(vls, []).append((s0, s1, tonn, bk, bru, lng, hoy, dim))

print(f"  Oppslag bygget for {len(idx)} veglenker.")


# ------------------------------
# KLASSIFISER ÅRSAKER
# ------------------------------
print("Klassifiserer årsaker...")

out_fields = [
    ID_FIELD, "STARTPOS", "SLUTTPOS",
    "AARSAK_DETALJERT",
    "TONN_PROP_VERDI", "VEG_BK_VERDI", "BRU_TONN_VERDI",
    "MAKS_LENGDE_VERDI", "FRI_HOYDE_VERDI",
    "DIM_KILDE",
]

updated = 0
no_hit  = 0
ok_cnt  = 0

with arcpy.da.UpdateCursor(OUT_FC, out_fields) as ucur:
    for row in ucur:
        vls = int(row[0])
        s0  = float(row[1] or 0.0)
        s1  = float(row[2] or 1.0)

        hits = [
            h for h in idx.get(vls, [])
            if overlap(s0, s1, h[0], h[1], strict=STRICT_OVERLAP)
        ]

        if not hits:
            no_hit += 1
            continue

        tonn_prop = min_or_none([h[2] for h in hits])
        bk_val    = min_or_none([h[3] for h in hits])
        bru_tonn  = min_or_none([h[4] for h in hits])
        maks_len  = min_or_none([h[5] for h in hits])
        fri_hoyde = min_or_none([h[6] for h in hits])

        # DIM_KILDE: fra felt hvis tilgjengelig, ellers beregn fra BK vs BRU
        if P_DIM:
            dim_kilde = "BRU" if any(h[7] == "BRU" for h in hits) else "VEG"
        else:
            if bk_val is not None and bru_tonn is not None:
                dim_kilde = "BRU" if bru_tonn <= bk_val else "VEG"
            elif bru_tonn is not None:
                dim_kilde = "BRU"
            else:
                dim_kilde = "VEG"

        tags = []

        # --- Vekt < 50t ---
        if tonn_prop is not None and float(tonn_prop) < VEKT_KRAV:
            if bk_val is not None and bru_tonn is not None and bk_val == bru_tonn:
                tags.extend(["BRU", "VEG"])
            elif dim_kilde == "BRU":
                tags.append("BRU")
            else:
                tags.append("VEG")

        # --- Bru 50–59t: selvstendig begrensning ---
        if bru_tonn is not None and float(bru_tonn) < BRU_TONN_KRAV:
            if "BRU" not in tags:
                tags.append("BRU60")

        # --- Lengde < 19.5m ---
        if maks_len is not None and float(maks_len) < LENGDE_KRAV:
            tags.append("LENGDE")

        # --- Høyde < 4.5m ---
        if fri_hoyde is not None and float(fri_hoyde) < HOYDE_KRAV:
            tags.append("HØYDE")

        aarsak = ", ".join(tags) if tags else "OK"
        if aarsak == "OK":
            ok_cnt += 1

        ucur.updateRow((
            vls, s0, s1,
            aarsak,
            tonn_prop, bk_val, bru_tonn,
            maks_len, fri_hoyde,
            dim_kilde,
        ))
        updated += 1

print(f"✅ Ferdig! Oppdaterte {updated} rader.")
if no_hit:
    print(f"  ⚠️  {no_hit} rader uten profil-treff (ingen overlapp funnet).")
if ok_cnt:
    print(f"  ⚠️  {ok_cnt} rader fikk AARSAK = 'OK' — disse hadde ingen verdi under terskel.")
    print(f"      Sjekk om TONN_PROP / LEN_PROP er NULL for disse segmentene.")
else:
    print(f"  ✅ Ingen 'OK'-rader — alle flaskehalser har klassifisert årsak.")
