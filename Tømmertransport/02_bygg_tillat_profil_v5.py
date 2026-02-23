# 02_bygg_tillat_profil.py
from __future__ import annotations

import os
from math import inf
from typing import Dict, Optional

import arcpy

from config import GDB, KJORETOY_TOMMER
from naming import fc
from fields import ensure_field

ID_FIELD = "VEGLENKESEKV_ID"
arcpy.env.overwriteOutput = True

def min_update(cur: Optional[float], val: Optional[float]) -> Optional[float]:
    if val is None:
        return cur
    return val if cur is None else min(cur, val)

class Stats:
    __slots__ = ("veg_tonn", "bru_tonn", "maks_len", "min_hoy")
    def __init__(self) -> None:
        self.veg_tonn: Optional[float] = None
        self.bru_tonn: Optional[float] = None
        self.maks_len: Optional[float] = None
        self.min_hoy: Optional[float] = None

def collect_stats(vegnett_fc: str, bru_fc: str, bk_fc: str, hoyde_fc: Optional[str]) -> Dict[int, Stats]:
    stats: Dict[int, Stats] = {}

    def get(vid: int) -> Stats:
        s = stats.get(vid)
        if s is None:
            s = Stats()
            stats[vid] = s
        return s

    # Bruksklasse: vekt + lengde
    with arcpy.da.SearchCursor(bk_fc, [ID_FIELD, "BK_VERDI", "MAKS_LENGDE"]) as cur:
        for vid, bk, ml in cur:
            s = get(int(vid))
            s.veg_tonn = min_update(s.veg_tonn, bk)
            s.maks_len = min_update(s.maks_len, ml)

    # Bruer: vekt
    with arcpy.da.SearchCursor(bru_fc, [ID_FIELD, "TILLATT_TONN"]) as cur:
        for vid, bt in cur:
            s = get(int(vid))
            s.bru_tonn = min_update(s.bru_tonn, bt)

    # Høyde (valgfritt)
    if hoyde_fc and arcpy.Exists(hoyde_fc):
        with arcpy.da.SearchCursor(hoyde_fc, [ID_FIELD, "MIN_HOYDE"]) as cur:
            for vid, h in cur:
                s = get(int(vid))
                s.min_hoy = min_update(s.min_hoy, h)

    return stats

def build_profile(out_fc: str, krav: dict[str, float]) -> None:
    vegnett_fc = fc(GDB, "Vegnett")
    bru_fc     = fc(GDB, "Bruer")
    bk_fc      = fc(GDB, "Bruksklasse")
    hoyde_fc   = fc(GDB, "Hoydebegrensning_LAV") if arcpy.Exists(fc(GDB, "Hoydebegrensning_LAV")) \
                 else (fc(GDB, "Hoydebegrensning") if arcpy.Exists(fc(GDB, "Hoydebegrensning")) else None)

    stats = collect_stats(vegnett_fc, bru_fc, bk_fc, hoyde_fc)

    n_hoy = sum(1 for s in stats.values() if s.min_hoy is not None)
    print(f"INFO: Høyde‑lag: {hoyde_fc if hoyde_fc else '(ingen)'}")
    print(f"INFO: Veglenker med høydebegrensning registrert: {n_hoy}")

    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)
    arcpy.management.CopyFeatures(vegnett_fc, out_fc)

    # Felt som fylles
    ensure_field(out_fc, "TILLATT_TONN", "DOUBLE")
    ensure_field(out_fc, "MAKS_LENGDE", "DOUBLE")
    ensure_field(out_fc, "MIN_HOYDE", "DOUBLE")

    ensure_field(out_fc, "FLASKEHALS_VEG", "TEXT", 10)
    ensure_field(out_fc, "FLASKEHALS_BRU", "TEXT", 10)
    ensure_field(out_fc, "FLASKEHALS_LENGDE", "TEXT", 10)
    ensure_field(out_fc, "FLASKEHALS_HOYDE", "TEXT", 10)

    ensure_field(out_fc, "DIM_KILDE", "TEXT", 10)

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

    with arcpy.da.UpdateCursor(out_fc, fields) as cur:
        for row in cur:
            s = stats.get(int(row[0]))
            if not s:
                continue

            # Propagerte verdier
            row[1] = s.veg_tonn
            row[2] = s.maks_len
            row[3] = s.min_hoy

            # Flaskehals‑flagg
            fh_veg = s.veg_tonn is not None and s.veg_tonn < krav["TONN"]
            fh_bru = s.bru_tonn is not None and s.bru_tonn < krav["TONN"]
            fh_len = s.maks_len is not None and s.maks_len < krav["LENGDE"]
            fh_hoy = s.min_hoy is not None and s.min_hoy < krav["HOYDE"]

            row[4] = "JA" if fh_veg else "NEI"
            row[5] = "JA" if fh_bru else "NEI"
            row[6] = "JA" if fh_len else "NEI"
            row[7] = "JA" if fh_hoy else "NEI"

            # DIM_KILDE: minste margin vinner
            margins = {
                "VEG":    (s.veg_tonn - krav["TONN"]) if s.veg_tonn is not None else inf,
                "BRU":    (s.bru_tonn - krav["TONN"]) if s.bru_tonn is not None else inf,
                "LENGDE": (s.maks_len - krav["LENGDE"]) if s.maks_len is not None else inf,
                "HOYDE":  (s.min_hoy  - krav["HOYDE"]) if s.min_hoy  is not None else inf,
            }
            row[8] = min(margins.items(), key=lambda kv: kv[1])[0]

            cur.updateRow(row)

    print("✅ Veg_TillatProfil ferdig bygget.")

if __name__ == "__main__":
    build_profile(out_fc=fc(GDB, "Veg_TillatProfil"), krav=KJORETOY_TOMMER)