# 03_korridor_dim_kilde.py
#
# Bygger:
# 1) Veg_TillatSegmentert  – kopi av Veg_TillatProfil
# 2) Veg_TillatKorridor    – dissolve per VEGLENKESEKV_ID
#
# Beholder:
# - FLASKEHALS_* (OR-propagert)
# - DIM_KILDE = BRU | VEG | LENGDE | HØYDE (strengeste årsak)

from __future__ import annotations

import arcpy
import os
from math import inf
from typing import Dict, Set


# -------------------------
# KONFIG
# -------------------------
GDB = r"D:\Conda\Flaskehalser\gdb\nvdb_radata.gdb"

IN_FC = os.path.join(GDB, "Veg_TillatProfil")

OUT_SEG_FC = os.path.join(GDB, "Veg_TillatSegmentert")
OUT_KORR_FC = os.path.join(GDB, "Veg_TillatKorridor")

ID_FIELD = "VEGLENKESEKV_ID"

arcpy.env.overwriteOutput = True


# -------------------------
# KJØRETØYPROFIL (må matche 02)
# -------------------------
KJORETOY = {
    "TONN": 60,
    "LENGDE": 24.0,
    "HOYDE": 4.2,
}


# -------------------------
# HJELP
# -------------------------
def ensure_field(fc, name, ftype, length=None):
    if name not in {f.name for f in arcpy.ListFields(fc)}:
        arcpy.management.AddField(fc, name, ftype, field_length=length)


class CorrStats:
    __slots__ = (
        "veg_tonn", "bru_tonn", "maks_len", "min_hoy",
        "fh_veg", "fh_bru", "fh_len", "fh_hoy",
        "dim_kilder"
    )

    def __init__(self):
        self.veg_tonn = None
        self.bru_tonn = None
        self.maks_len = None
        self.min_hoy = None

        self.fh_veg = False
        self.fh_bru = False
        self.fh_len = False
        self.fh_hoy = False

        self.dim_kilder: Set[str] = set()


def min_update(cur, val):
    if val is None:
        return cur
    return val if cur is None else min(cur, val)


# -------------------------
# SAMLE STATISTIKK PER KORRIDOR
# -------------------------
def collect_corridor_stats() -> Dict[int, CorrStats]:
    stats: Dict[int, CorrStats] = {}

    def get(vid):
        if vid not in stats:
            stats[vid] = CorrStats()
        return stats[vid]

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

    with arcpy.da.SearchCursor(IN_FC, fields) as cur:
        for (
            vid, veg_t, maks_l, min_h,
            fh_veg, fh_bru, fh_len, fh_hoy,
            dim
        ) in cur:

            s = get(int(vid))

            s.veg_tonn = min_update(s.veg_tonn, veg_t)
            s.maks_len = min_update(s.maks_len, maks_l)
            s.min_hoy = min_update(s.min_hoy, min_h)

            if fh_veg == "JA":
                s.fh_veg = True
            if fh_bru == "JA":
                s.fh_bru = True
            if fh_len == "JA":
                s.fh_len = True
            if fh_hoy == "JA":
                s.fh_hoy = True

            if dim:
                s.dim_kilder.add(dim)

    return stats


# -------------------------
# 1) SEGMENTERT OUTPUT
# -------------------------
def build_segment_output():
    if arcpy.Exists(OUT_SEG_FC):
        arcpy.management.Delete(OUT_SEG_FC)

    arcpy.management.CopyFeatures(IN_FC, OUT_SEG_FC)
    print("✅ Veg_TillatSegmentert ferdig.")


# -------------------------
# 2) KORRIDOR OUTPUT
# -------------------------
def build_corridor_output():
    stats = collect_corridor_stats()

    if arcpy.Exists(OUT_KORR_FC):
        arcpy.management.Delete(OUT_KORR_FC)

    # Dissolve – én linje per veglenke
    arcpy.management.Dissolve(
        in_features=IN_FC,
        out_feature_class=OUT_KORR_FC,
        dissolve_field=ID_FIELD,
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES",
    )

    # Felter
    ensure_field(OUT_KORR_FC, "FLASKEHALS_VEG", "TEXT", 10)
    ensure_field(OUT_KORR_FC, "FLASKEHALS_BRU", "TEXT", 10)
    ensure_field(OUT_KORR_FC, "FLASKEHALS_LENGDE", "TEXT", 10)
    ensure_field(OUT_KORR_FC, "FLASKEHALS_HOYDE", "TEXT", 10)
    ensure_field(OUT_KORR_FC, "DIM_KILDE", "TEXT", 10)

    with arcpy.da.UpdateCursor(
        OUT_KORR_FC,
        [
            ID_FIELD,
            "FLASKEHALS_VEG",
            "FLASKEHALS_BRU",
            "FLASKEHALS_LENGDE",
            "FLASKEHALS_HOYDE",
            "DIM_KILDE",
        ],
    ) as cur:

        for row in cur:
            vid = int(row[0])
            s = stats.get(vid)

            if not s:
                continue

            row[1] = "JA" if s.fh_veg else "NEI"
            row[2] = "JA" if s.fh_bru else "NEI"
            row[3] = "JA" if s.fh_len else "NEI"
            row[4] = "JA" if s.fh_hoy else "NEI"

            # --- DIM_KILDE: strengeste margin vinner ---
            margins = {
                "VEG":     (s.veg_tonn - KJORETOY["TONN"]) if s.veg_tonn is not None else inf,
                "BRU":     (s.bru_tonn - KJORETOY["TONN"]) if s.bru_tonn is not None else inf,
                "LENGDE":  (s.maks_len - KJORETOY["LENGDE"]) if s.maks_len is not None else inf,
                "HØYDE":   (s.min_hoy - KJORETOY["HOYDE"]) if s.min_hoy is not None else inf,
            }

            row[5] = min(margins.items(), key=lambda kv: kv[1])[0]

            cur.updateRow(row)

    print("✅ Veg_TillatKorridor ferdig.")


# -------------------------
# MAIN
# -------------------------
def main():
    build_segment_output()
    build_corridor_output()


if __name__ == "__main__":
    main()