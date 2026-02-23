# 02_bygg_tillat_profil.py
#
# Bygger Veg_TillatProfil med:
# - Propagerte min-verdier per VEGLENKESEKV_ID
# - Flaskehals-flagg per dimensjon
# - DIM_KILDE = BRU | VEG | LENGDE | HOYDE
#
# Tilpasset tømmertransport (24.0 m), lett å endre kjøretøyprofil

from __future__ import annotations

import os
from math import inf
from typing import Dict, Optional, Literal

import arcpy

# -------------------------
# KONFIG
# -------------------------
# >>> OPPDATER denne stien hvis GDB ligger et annet sted <<<
GDB = r"D:\Conda\Flaskehalser\gdb\nvdb_radata.gdb"

VEGNETT_FC = os.path.join(GDB, "Vegnett")
BRU_FC     = os.path.join(GDB, "Bruer")
BK_FC      = os.path.join(GDB, "Bruksklasse")

# Høyde: bruk LAV (fra 06) hvis den finnes, ellers evt. standard
_HCANDS = [
    os.path.join(GDB, "Hoydebegrensning_LAV"),
    os.path.join(GDB, "Hoydebegrensning"),
]
HOYDE_FC = next((c for c in _HCANDS if arcpy.Exists(c)), None)

OUT_FC = os.path.join(GDB, "Veg_TillatProfil")
ID_FIELD = "VEGLENKESEKV_ID"

arcpy.env.overwriteOutput = True

# -------------------------
# KJØRETØYPROFIL
# -------------------------
KJORETOY = {
    "NAVN": "TOMMER",
    "TONN": 60.0,    # krav totalvekt (tilpass ved behov)
    "LENGDE": 24.0,  # meter
    "HOYDE": 4.2,    # meter
}

# -------------------------
# HJELP
# -------------------------
FieldType = Literal[
    "SHORT",
    "LONG",
    "BIGINTEGER",
    "FLOAT",
    "DOUBLE",
    "TEXT",
    "DATE",
    "DATEHIGHPRECISION",
    "DATEONLY",
    "TIMEONLY",
    "TIMESTAMPOFFSET",
    "BLOB",
    "GUID",
    "RASTER",
]

def ensure_field(fc: str, name: str, ftype: FieldType, length: Optional[int] = None) -> None:
    """Oppretter felt hvis det ikke finnes fra før (Pylance‑ren signatur)."""
    existing = {f.name for f in arcpy.ListFields(fc)}
    if name in existing:
        return
    arcpy.management.AddField(fc, name, ftype, field_length=length)

class Stats:
    __slots__ = ("veg_tonn", "bru_tonn", "maks_len", "min_hoy")
    def __init__(self) -> None:
        self.veg_tonn: Optional[float] = None
        self.bru_tonn: Optional[float] = None
        self.maks_len: Optional[float] = None
        self.min_hoy: Optional[float] = None

def min_update(cur: Optional[float], val: Optional[float]) -> Optional[float]:
    if val is None:
        return cur
    return val if cur is None else min(cur, val)

# -------------------------
# LES INN DATA
# -------------------------
def collect_stats() -> Dict[int, Stats]:
    stats: Dict[int, Stats] = {}

    def get(vid: int) -> Stats:
        s = stats.get(vid)
        if s is None:
            s = Stats()
            stats[vid] = s
        return s

    # --- Bruksklasse: vekt + lengde ---
    with arcpy.da.SearchCursor(BK_FC, [ID_FIELD, "BK_VERDI", "MAKS_LENGDE"]) as cur:
        for vid, bk, ml in cur:
            s = get(int(vid))
            s.veg_tonn = min_update(s.veg_tonn, bk)
            s.maks_len = min_update(s.maks_len, ml)

    # --- Bruer: vekt ---
    with arcpy.da.SearchCursor(BRU_FC, [ID_FIELD, "TILLATT_TONN"]) as cur:
        for vid, bt in cur:
            s = get(int(vid))
            s.bru_tonn = min_update(s.bru_tonn, bt)

    # --- Høyde (valgfritt) ---
    if HOYDE_FC and arcpy.Exists(HOYDE_FC):
        with arcpy.da.SearchCursor(HOYDE_FC, [ID_FIELD, "MIN_HOYDE"]) as cur:
            for vid, h in cur:
                s = get(int(vid))
                s.min_hoy = min_update(s.min_hoy, h)

    return stats

# -------------------------
# BYGG PROFIL
# -------------------------
def build_profile() -> None:
    stats = collect_stats()

    # Sanity‑logg: hvor mange lenker har faktisk høyde?
    n_hoy = sum(1 for s in stats.values() if s.min_hoy is not None)
    chosen_hoyde_fc = HOYDE_FC if HOYDE_FC else "(ingen)"
    print(f"INFO: Høyde‑lag: {chosen_hoyde_fc}")
    print(f"INFO: Veglenker med høydebegrensning registrert: {n_hoy}")

    if arcpy.Exists(OUT_FC):
        arcpy.management.Delete(OUT_FC)

    arcpy.management.CopyFeatures(VEGNETT_FC, OUT_FC)

    # --- Felter som fylles ---
    ensure_field(OUT_FC, "TILLATT_TONN", "DOUBLE")
    ensure_field(OUT_FC, "MAKS_LENGDE", "DOUBLE")
    ensure_field(OUT_FC, "MIN_HOYDE", "DOUBLE")

    ensure_field(OUT_FC, "FLASKEHALS_VEG", "TEXT", 10)
    ensure_field(OUT_FC, "FLASKEHALS_BRU", "TEXT", 10)
    ensure_field(OUT_FC, "FLASKEHALS_LENGDE", "TEXT", 10)
    ensure_field(OUT_FC, "FLASKEHALS_HOYDE", "TEXT", 10)

    ensure_field(OUT_FC, "DIM_KILDE", "TEXT", 10)

    fields = [
        ID_FIELD,
        "TILLATT_TONN",
        "MAKS_LENGDE",
        "MIN_HOYDE",
        "FLASKEHALS_VEG",
        "FLASKEHALS_BRU",
        "FLASKEHALS_LENGDE",
        "FLASKEHALS_HOYDE",
        "DIM_KILDE",
    ]

    with arcpy.da.UpdateCursor(OUT_FC, fields) as cur:
        for row in cur:
            vid = int(row[0])
            s = stats.get(vid)
            if not s:
                continue

            # Propagerte verdier
            row[1] = s.veg_tonn
            row[2] = s.maks_len
            row[3] = s.min_hoy

            # Flaskehals-flagg
            fh_veg = s.veg_tonn is not None and s.veg_tonn < KJORETOY["TONN"]
            fh_bru = s.bru_tonn is not None and s.bru_tonn < KJORETOY["TONN"]
            fh_len = s.maks_len is not None and s.maks_len < KJORETOY["LENGDE"]
            fh_hoy = s.min_hoy is not None and s.min_hoy < KJORETOY["HOYDE"]

            row[4] = "JA" if fh_veg else "NEI"
            row[5] = "JA" if fh_bru else "NEI"
            row[6] = "JA" if fh_len else "NEI"
            row[7] = "JA" if fh_hoy else "NEI"

            # DIM_KILDE: minste margin vinner
            margins = {
                "VEG":    (s.veg_tonn - KJORETOY["TONN"]) if s.veg_tonn is not None else inf,
                "BRU":    (s.bru_tonn - KJORETOY["TONN"]) if s.bru_tonn is not None else inf,
                "LENGDE": (s.maks_len - KJORETOY["LENGDE"]) if s.maks_len is not None else inf,
                "HOYDE":  (s.min_hoy  - KJORETOY["HOYDE"]) if s.min_hoy  is not None else inf,
            }
            row[8] = min(margins.items(), key=lambda kv: kv[1])[0]

            cur.updateRow(row)

    print("✅ Veg_TillatProfil ferdig bygget.")

# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    build_profile()