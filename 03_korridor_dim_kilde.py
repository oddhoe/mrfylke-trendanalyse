# 03_korridor_dim_kilde.py
from __future__ import annotations

import arcpy
from math import inf
from typing import Dict

from config import GDB, KJORETOY_TOMMER
from naming import fc
from fields import ensure_field

ID_FIELD = "VEGLENKESEKV_ID"
arcpy.env.overwriteOutput = True

class CorrStats:
    __slots__ = ("veg_tonn", "bru_tonn", "maks_len", "min_hoy", "fh_veg", "fh_bru", "fh_len", "fh_hoy")
    def __init__(self) -> None:
        self.veg_tonn = self.bru_tonn = self.maks_len = self.min_hoy = None
        self.fh_veg = self.fh_bru = self.fh_len = self.fh_hoy = False

def min_update(cur, val):
    if val is None:
        return cur
    return val if cur is None else min(cur, val)

def collect_corridor_stats(in_fc: str) -> Dict[int, CorrStats]:
    stats: Dict[int, CorrStats] = {}

    def get(vid: int) -> CorrStats:
        s = stats.get(vid)
        if s is None:
            s = CorrStats()
            stats[vid] = s
        return s

    fields = [
        ID_FIELD,
        "TILLATT_TONN",
        "MAKS_LENGDE",
        "MIN_HOYDE",
        "FLASKEHALS_VEG",
        "FLASKEHALS_BRU",
        "FLASKEHALS_LENGDE",
        "FLASKEHALS_HOYDE",
    ]

    with arcpy.da.SearchCursor(in_fc, fields) as cur:
        for vid, veg_t, maks_l, min_h, fhv, fhb, fhl, fhh in cur:
            s = get(int(vid))
            s.veg_tonn = min_update(s.veg_tonn, veg_t)
            s.maks_len = min_update(s.maks_len, maks_l)
            s.min_hoy = min_update(s.min_hoy, min_h)
            if fhv == "JA": s.fh_veg = True
            if fhb == "JA": s.fh_bru = True
            if fhl == "JA": s.fh_len = True
            if fhh == "JA": s.fh_hoy = True
    return stats

def build_segment_and_corridor(in_fc: str, out_seg_fc: str, out_korr_fc: str, krav: dict[str, float]) -> None:
    # 1) Segmentert
    if arcpy.Exists(out_seg_fc):
        arcpy.management.Delete(out_seg_fc)
    arcpy.management.CopyFeatures(in_fc, out_seg_fc)
    print("✅ Veg_TillatSegmentert ferdig.")

    # 2) Korridor (dissolve per lenke)
    stats = collect_corridor_stats(in_fc)
    if arcpy.Exists(out_korr_fc):
        arcpy.management.Delete(out_korr_fc)

    arcpy.management.Dissolve(
        in_features=in_fc,
        out_feature_class=out_korr_fc,
        dissolve_field=ID_FIELD,
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES",
    )

    ensure_field(out_korr_fc, "FLASKEHALS_VEG", "TEXT", 10)
    ensure_field(out_korr_fc, "FLASKEHALS_BRU", "TEXT", 10)
    ensure_field(out_korr_fc, "FLASKEHALS_LENGDE", "TEXT", 10)
    ensure_field(out_korr_fc, "FLASKEHALS_HOYDE", "TEXT", 10)
    ensure_field(out_korr_fc, "DIM_KILDE", "TEXT", 10)

    with arcpy.da.UpdateCursor(
        out_korr_fc,
        [ID_FIELD, "FLASKEHALS_VEG", "FLASKEHALS_BRU", "FLASKEHALS_LENGDE", "FLASKEHALS_HOYDE", "DIM_KILDE"],
    ) as cur:
        for row in cur:
            s = stats.get(int(row[0]))
            if not s:
                continue
            row[1] = "JA" if s.fh_veg else "NEI"
            row[2] = "JA" if s.fh_bru else "NEI"
            row[3] = "JA" if s.fh_len else "NEI"
            row[4] = "JA" if s.fh_hoy else "NEI"

            margins = {
                "VEG":    (s.veg_tonn - krav["TONN"]) if s.veg_tonn is not None else inf,
                "BRU":    (s.bru_tonn - krav["TONN"]) if s.bru_tonn is not None else inf,
                "LENGDE": (s.maks_len - krav["LENGDE"]) if s.maks_len is not None else inf,
                "HOYDE":  (s.min_hoy  - krav["HOYDE"]) if s.min_hoy  is not None else inf,
            }
            row[5] = min(margins.items(), key=lambda kv: kv[1])[0]
            cur.updateRow(row)

    print("✅ Veg_TillatKorridor ferdig.")

if __name__ == "__main__":
    in_fc      = fc(GDB, "Veg_TillatProfil")
    out_seg_fc = fc(GDB, "Veg_TillatSegmentert")
    out_korr_fc= fc(GDB, "Veg_TillatKorridor")
    build_segment_and_corridor(in_fc, out_seg_fc, out_korr_fc, KJORETOY_TOMMER)