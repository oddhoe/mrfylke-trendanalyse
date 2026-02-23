# 05_summering.py
from __future__ import annotations

import os
from typing import Dict, Any
import arcpy

from config import GDB
from naming import fc

arcpy.env.overwriteOutput = True

IN_FC = fc(GDB, "Veg_TillatProfil")
VEGNETT_FC = fc(GDB, "Vegnett")

OUT_DIR = os.path.join(os.path.dirname(GDB), "reports")
os.makedirs(OUT_DIR, exist_ok=True)
OUT_XLSX = os.path.join(OUT_DIR, "flaskehalser_summering.xlsx")
OUT_CSV_COMMUNE = os.path.join(OUT_DIR, "flaskehalser_pr_kommune.csv")
OUT_CSV_VEGSYS  = os.path.join(OUT_DIR, "flaskehalser_pr_vegsystem.csv")

# Hvilke flagg vi rapporterer på
CAUSE_FIELDS = [
    ("BRU",    "FLASKEHALS_BRU"),
    ("VEG",    "FLASKEHALS_VEG"),
    ("LENGDE", "FLASKEHALS_LENGDE"),
    ("HOYDE",  "FLASKEHALS_HOYDE"),
]
ALL_ALIAS = "ALLE"

def add_commune_code_if_missing() -> str:
    """Sikre at profil-laget har en kommune-kode å gruppere på."""
    # Prøv å bruke eksisterende felt (tilpass navn om du har KOMMUNENR/KOMMUNE)
    candidate_fields = {"KOMMUNENR", "KOMMUNE_NR", "KOMMUNE", "KOMMUNENUMMER"}
    existing = {f.name.upper() for f in arcpy.ListFields(IN_FC)}
    found = next((f for f in candidate_fields if f in existing), None)
    if found:
        return next(x.name for x in arcpy.ListFields(IN_FC) if x.name.upper() == found)
    # Ellers gjør en romlig join fra Vegnett (som antas å ha kommune-felt)
    # Vi forsøker "KOMMUNENR" fra Vegnett → over til profil
    veg_fields = {f.name.upper() for f in arcpy.ListFields(VEGNETT_FC)}
    src_field = "KOMMUNENR" if "KOMMUNENR" in veg_fields else None
    if src_field is None:
        print("⚠️ Fant ikke kommune-felt i Vegnett. Hopper over kommune-summering.")
        return ""

    tmp_join = fc(GDB, "tmp__profil_kommune_join")
    if arcpy.Exists(tmp_join):
        arcpy.management.Delete(tmp_join)

    arcpy.analysis.SpatialJoin(
        target_features=IN_FC,
        join_features=VEGNETT_FC,
        out_feature_class=tmp_join,
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        match_option="INTERSECT",
        field_mapping=None,
    )
    # Kopier tilbake (kun ønsket felt)
    # Vi lager et stabilt feltnavn i profil: KOMMUNENR
    if "KOMMUNENR" not in {f.name for f in arcpy.ListFields(IN_FC)}:
        arcpy.management.AddField(IN_FC, "KOMMUNENR", "TEXT", field_length=4)

    # Bygg lookup fra OID i target til kommune
    # OBS: SpatialJoin lager mange feltnavn; finn join-feltet som matcher src_field
    join_field = next((f.name for f in arcpy.ListFields(tmp_join) if f.name.upper() == src_field), None)
    oid_field = arcpy.Describe(IN_FC).OIDFieldName
    join_oid_field = arcpy.Describe(tmp_join).OIDFieldName

    # Map OID → kommune
    lut: Dict[Any, Any] = {}
    with arcpy.da.SearchCursor(tmp_join, [oid_field, join_field]) as cur:
        for oid_val, komm in cur:
            lut[oid_val] = komm

    with arcpy.da.UpdateCursor(IN_FC, [oid_field, "KOMMUNENR"]) as cur:
        for oid_val, _ in cur:
            cur.updateRow((oid_val, lut.get(oid_val)))

    arcpy.management.Delete(tmp_join)
    return "KOMMUNENR"

def length_km(shape) -> float:
    # For forutsigbarhet, bruk SHAPE@LENGTH (forutsetter meter → del på 1000)
    try:
        return float(shape.length) / 1000.0
    except Exception:
        return 0.0

def summarize_by(field_name: str) -> Dict[str, Dict[str, float]]:
    """
    Returnerer:
      { group_value: { 'ANTALL_ALLE': x, 'KM_ALLE': y, 'ANTALL_BRU':..., 'KM_BRU':..., ... } }
    """
    # Sett opp tomt resultat
    res: Dict[str, Dict[str, float]] = {}
    def ensure(group: str) -> Dict[str, float]:
        if group not in res:
            res[group] = {
                "ANTALL_ALLE": 0, "KM_ALLE": 0.0,
                **{f"ANTALL_{k}": 0 for k,_ in CAUSE_FIELDS},
                **{f"KM_{k}": 0.0 for k,_ in CAUSE_FIELDS},
            }
        return res[group]

    fields = [
        field_name, "SHAPE@",  # gruppe og geometri
        "FLASKEHALS_BRU", "FLASKEHALS_VEG", "FLASKEHALS_LENGDE", "FLASKEHALS_HOYDE",
    ]
    with arcpy.da.SearchCursor(IN_FC, fields) as cur:
        for grp, shp, fh_bru, fh_veg, fh_len, fh_hoy in cur:
            key = str(grp) if grp is not None else "Ukjent"
            d = ensure(key)
            ln = length_km(shp)

            # tell total
            if any(v == "JA" for v in (fh_bru, fh_veg, fh_len, fh_hoy)):
                d["ANTALL_ALLE"] += 1
                d["KM_ALLE"] += ln

            # tell pr. årsak
            if fh_bru == "JA":
                d["ANTALL_BRU"] += 1; d["KM_BRU"] += ln
            if fh_veg == "JA":
                d["ANTALL_VEG"] += 1; d["KM_VEG"] += ln
            if fh_len == "JA":
                d["ANTALL_LENGDE"] += 1; d["KM_LENGDE"] += ln
            if fh_hoy == "JA":
                d["ANTALL_HOYDE"] += 1; d["KM_HOYDE"] += ln

    return res

def write_csv(path: str, data: Dict[str, Dict[str, float]], group_header: str) -> None:
    import csv
    headers = [group_header, "ANTALL_ALLE", "KM_ALLE",
               "ANTALL_BRU", "KM_BRU", "ANTALL_VEG", "KM_VEG",
               "ANTALL_LENGDE", "KM_LENGDE", "ANTALL_HOYDE", "KM_HOYDE"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(headers)
        for grp, vals in sorted(data.items()):
            w.writerow([grp] + [round(vals[h], 3) if "KM_" in h else vals[h] for h in headers[1:]])

def write_excel(path: str, by_commune: Dict[str, Dict[str, float]], by_vegsys: Dict[str, Dict[str, float]]) -> None:
    try:
        import pandas as pd
    except Exception:
        print("⚠️ pandas ikke tilgjengelig – hopper over Excel. CSV er skrevet.")
        return

    def to_df(data: Dict[str, Dict[str, float]], group_header: str):
        import pandas as pd  # type: ignore
        rows = []
        for grp, vals in data.items():
            row = {"Gruppe": grp} | vals
            rows.append(row)
        df = pd.DataFrame(rows)
        df.rename(columns={"Gruppe": group_header}, inplace=True)
        # sorter litt penere
        return df.sort_values(by=[group_header])

    df_commune = to_df(by_commune, "KOMMUNENR")
    df_vegsys  = to_df(by_vegsys, "VEGSYSTEM")

    with pd.ExcelWriter(path, engine="openpyxl") as xw:  # type: ignore
        df_commune.to_excel(xw, sheet_name="Kommune", index=False)
        df_vegsys.to_excel(xw,  sheet_name="Vegsystem", index=False)
    print(f"📄 Excel skrevet: {path}")

def main() -> None:
    print("▶️ 05: Summerer flaskehalser …")

    # 0) Sikre kommune‑felt
    kommune_field = add_commune_code_if_missing()

    # 1) Kommune
    if kommune_field:
        by_commune = summarize_by(kommune_field)
        write_csv(OUT_CSV_COMMUNE, by_commune, "KOMMUNENR")
    else:
        by_commune = {}

    # 2) Vegsystem (kategori+nummer fra Vegnett → join‑felt i profil)
    # Hvis de feltene finnes i profil: bruk dem. Hvis ikke, SpatialJoin én gang til (lett å legge på her).
    # Prøv direkte først:
    prof_fields = {f.name.upper() for f in arcpy.ListFields(IN_FC)}
    if "VEGKATEGORI" in prof_fields and "VEGNUMMER" in prof_fields:
        # Lag en sammensatt nøkkel i flyt (uten å skrive felt)
        tmp_res: Dict[str, Dict[str, float]] = {}
        fields = ["VEGKATEGORI", "VEGNUMMER", "SHAPE@", "FLASKEHALS_BRU", "FLASKEHALS_VEG", "FLASKEHALS_LENGDE", "FLASKEHALS_HOYDE"]
        with arcpy.da.SearchCursor(IN_FC, fields) as cur:
            for kat, nr, shp, fh_bru, fh_veg, fh_len, fh_hoy in cur:
                key = f"{kat or '?'}{nr or ''}"
                ln = length_km(shp)
                d = tmp_res.setdefault(key, {
                    "ANTALL_ALLE": 0, "KM_ALLE": 0.0,
                    "ANTALL_BRU": 0, "KM_BRU": 0.0,
                    "ANTALL_VEG": 0, "KM_VEG": 0.0,
                    "ANTALL_LENGDE": 0, "KM_LENGDE": 0.0,
                    "ANTALL_HOYDE": 0, "KM_HOYDE": 0.0,
                })
                if any(v == "JA" for v in (fh_bru, fh_veg, fh_len, fh_hoy)):
                    d["ANTALL_ALLE"] += 1; d["KM_ALLE"] += ln
                if fh_bru == "JA": d["ANTALL_BRU"] += 1; d["KM_BRU"] += ln
                if fh_veg == "JA": d["ANTALL_VEG"] += 1; d["KM_VEG"] += ln
                if fh_len == "JA": d["ANTALL_LENGDE"] += 1; d["KM_LENGDE"] += ln
                if fh_hoy == "JA": d["ANTALL_HOYDE"] += 1; d["KM_HOYDE"] += ln
        by_vegsys = tmp_res
    else:
        print("ℹ️ Fant ikke VEGKATEGORI/VEGNUMMER i profil – hopper over vegsystem-summering.")
        by_vegsys = {}

    # 3) Skriv Excel
    write_excel(OUT_XLSX, by_commune, by_vegsys)

    print(f"✅ 05: CSV skrevet til {OUT_CSV_COMMUNE} og {OUT_CSV_VEGSYS}")
    print("✅ 05: Ferdig.")

if __name__ == "__main__":
    main()