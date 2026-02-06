# 04_flaskehalser_lag.py
from __future__ import annotations

import arcpy
from typing import Optional
from config import GDB
from naming import fc
from fields import ensure_field

arcpy.env.overwriteOutput = True
ID_FIELD = "VEGLENKESEKV_ID"

IN_FC = fc(GDB, "Veg_TillatProfil")

# Mål-lag
OUT_ALL     = fc(GDB, "Flaskehalser_All")
OUT_BRU     = fc(GDB, "Flaskehals_Bru")
OUT_VEG     = fc(GDB, "Flaskehals_Veg")
OUT_LENGDE  = fc(GDB, "Flaskehals_Lengde")
OUT_HOYDE   = fc(GDB, "Flaskehals_Hoyde")

def make_subset(in_fc: str, out_fc: str, where: Optional[str], arsak: str) -> None:
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)
    if where:
        arcpy.analysis.Select(in_fc, out_fc, where_clause=where)
    else:
        arcpy.management.CopyFeatures(in_fc, out_fc)
    # merk årsak for lett symbolisering
    ensure_field(out_fc, "ARSAK", "TEXT", 20)
    with arcpy.da.UpdateCursor(out_fc, ["ARSAK"]) as cur:
        for row in cur:
            row[0] = arsak
            cur.updateRow(row)

def main() -> None:
    # 1) Samlet
    where_all = "FLASKEHALS_VEG = 'JA' OR FLASKEHALS_BRU = 'JA' OR FLASKEHALS_LENGDE = 'JA' OR FLASKEHALS_HOYDE = 'JA'"
    make_subset(IN_FC, OUT_ALL, where_all, "ALLE")

    # 2) Per årsak
    make_subset(IN_FC, OUT_BRU,    "FLASKEHALS_BRU = 'JA'",    "BRU")
    make_subset(IN_FC, OUT_VEG,    "FLASKEHALS_VEG = 'JA'",    "VEG")
    make_subset(IN_FC, OUT_LENGDE, "FLASKEHALS_LENGDE = 'JA'", "LENGDE")
    make_subset(IN_FC, OUT_HOYDE,  "FLASKEHALS_HOYDE = 'JA'",  "HOYDE")

    print("✅ 04: Flaskehals‑temalag opprettet.")

if __name__ == "__main__":
    main()