# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime
import math
import csv

import numpy as np

try:
    import arcpy
except Exception as e:
    print("FEIL: arcpy er ikke tilgjengelig. Kjør i ArcGIS Pro Python (arcgispro-py3).")
    raise


# -----------------------------
# Hjelpefunksjoner
# -----------------------------

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts}  {msg}")


def ensure_gdb(path):
    folder = os.path.dirname(path)
    name = os.path.basename(path)
    if not folder:
        folder = os.getcwd()
    if not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True)
    if not arcpy.Exists(path):
        log(f"Oppretter GDB: {path}")
        arcpy.management.CreateFileGDB(folder, name)
    return path


def fc_path(gdb, name_or_path):
    # Tillater både full sti og navn i gdb
    if arcpy.Exists(name_or_path):
        return name_or_path
    p = os.path.join(gdb, name_or_path)
    if arcpy.Exists(p):
        return p
    raise FileNotFoundError(f"Fant ikke featureclass/layer: {name_or_path}")


def list_fields(fc):
    return [f.name for f in arcpy.ListFields(fc)]


def pick_first_field(fc, candidates):
    fields = set(list_fields(fc))
    for c in candidates:
        if c in fields:
            return c
    return None


def safe_add_field(fc, name, ftype, length=None):
    fields = set(list_fields(fc))
    if name in fields:
        return
    if length and ftype.upper() in ("TEXT", "STRING"):
        arcpy.management.AddField(fc, name, ftype, field_length=length)
    else:
        arcpy.management.AddField(fc, name, ftype)


def percentile_safe(vals, p):
    if not vals:
        return None
    arr = np.array(vals, dtype=float)
    return float(np.percentile(arr, p))


def mean_safe(vals):
    if not vals:
        return None
    return float(np.mean(np.array(vals, dtype=float)))


def min_safe(vals):
    if not vals:
        return None
    return float(np.min(np.array(vals, dtype=float)))


def max_safe(vals):
    if not vals:
        return None
    return float(np.max(np.array(vals, dtype=float)))


def std_safe(vals):
    if not vals:
        return None
    arr = np.array(vals, dtype=float)
    if len(arr) < 2:
        return 0.0
    return float(np.std(arr, ddof=1))


def count_under(vals, thr):
    if not vals:
        return 0
    arr = np.array(vals, dtype=float)
    return int(np.sum(arr < thr))


def to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


# -----------------------------
# Kjerne: koble punkter til vegnett via GenerateNearTable
# -----------------------------

def build_near_mapping(in_points, in_lines, search_radius, out_table):
    """
    Lager mapping fra point OID -> line OID (nærmeste innen radius).
    Returnerer dict: {point_oid: line_oid}
    """
    for p in [out_table]:
        if arcpy.Exists(p):
            arcpy.management.Delete(p)

    log("Kobler punkter -> vegnett (GenerateNearTable)...")
    arcpy.analysis.GenerateNearTable(
        in_features=in_points,
        near_features=in_lines,
        out_table=out_table,
        search_radius=search_radius,
        location="NO_LOCATION",
        angle="NO_ANGLE",
        closest="CLOSEST",
        closest_count=1,
        method="PLANAR"
    )

    mapping = {}
    with arcpy.da.SearchCursor(out_table, ["IN_FID", "NEAR_FID"]) as cur:
        for in_fid, near_fid in cur:
            if in_fid is None or near_fid is None:
                continue
            mapping[int(in_fid)] = int(near_fid)
    return mapping


def aggregate_fwd_by_vegnett(fwd_fc, fwd_value_field, map_point_to_line):
    """
    Leser FWD-verdi per punkt, grupperer på vegnett_oid.
    Returnerer:
      stats_by_line: dict line_oid -> dict stats
      bad_point_oids: set point_oid (under terskel beregnes senere)
      point_value: dict point_oid -> value
    """
    oid_field = arcpy.Describe(fwd_fc).OIDFieldName
    stats_vals = {}   # line_oid -> list(values)
    point_value = {}

    with arcpy.da.SearchCursor(fwd_fc, [oid_field, fwd_value_field]) as cur:
        for oid, val in cur:
            oid = int(oid)
            v = to_float(val)
            if v is None:
                continue
            point_value[oid] = v
            line_oid = map_point_to_line.get(oid)
            if line_oid is None:
                continue
            stats_vals.setdefault(line_oid, []).append(v)

    stats_by_line = {}
    for line_oid, vals in stats_vals.items():
        stats_by_line[line_oid] = {
            "n": int(len(vals)),
            "min": min_safe(vals),
            "max": max_safe(vals),
            "mean": mean_safe(vals),
            "std": std_safe(vals),
        }
    return stats_by_line, point_value


def aggregate_bru_minbk_by_vegnett(bruer_fc, bru_bk_field, map_bru_to_line):
    """
    Returnerer dict line_oid -> min_bk (int)
    """
    if bru_bk_field is None:
        return {}

    oid_field = arcpy.Describe(bruer_fc).OIDFieldName
    minbk = {}  # line_oid -> int

    with arcpy.da.SearchCursor(bruer_fc, [oid_field, bru_bk_field]) as cur:
        for oid, bk in cur:
            v = to_float(bk)
            if v is None:
                continue
            v = int(round(v))
            line_oid = map_bru_to_line.get(int(oid))
            if line_oid is None:
                continue
            if line_oid not in minbk:
                minbk[line_oid] = v
            else:
                minbk[line_oid] = min(minbk[line_oid], v)

    return minbk


# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Fylkesvis screening BKT8/50 -> BKT8/60 basert på FWD + NVDB vegnett/bruer.")
    ap.add_argument("--fwd_gpkg", required=True, help="Sti til GPKG med FWD-punkter")
    ap.add_argument("--fwd_layer", required=True, help="Layer/featureclass-navn i GPKG (f.eks. 'fwd')")
    ap.add_argument("--fwd_value_field", required=True, help="Feltnavn for bæreevne i tonn (float)")

    ap.add_argument("--nvdb_gdb", required=True, help="Sti til NVDB FileGDB (ut fra nvdb_to_gdb.py)")
    ap.add_argument("--vegnett_fc", required=True, help="Vegnett featureclass (navn i gdb eller full sti)")
    ap.add_argument("--bruer_fc", required=True, help="Bruer featureclass (navn i gdb eller full sti)")

    ap.add_argument("--out_gdb", required=True, help="Output FileGDB")
    ap.add_argument("--out_vegnett_name", default="Vegnett_BKT8_60_screening", help="Navn på output vegnett i out_gdb")
    ap.add_argument("--out_fwd_avvik_name", default="FWD_avvik_under_terskel", help="Navn på output avvikspunkter i out_gdb")

    ap.add_argument("--threshold", type=float, default=10.0, help="Terskel for bæreevne (tonn). Default 10.0")
    ap.add_argument("--percentile", type=float, default=95.0, help="Persentil for vurdering (p95). Default 95")
    ap.add_argument("--min_fwd_n", type=int, default=5, help="Min antall FWD-punkt pr vegsegment for å vurdere. Default 5")
    ap.add_argument("--search_radius", type=float, default=20.0, help="Radius (meter) for å knytte FWD/bruer til vegnett. Default 20")
    ap.add_argument("--csv_summary", default=None, help="Valgfritt: sti til CSV-sammendrag")

    ap.add_argument("--vegnett_bk_field", default=None, help="Valgfritt: BK-felt på vegnett (ellers autodetekteres)")
    ap.add_argument("--bru_bk_field", default=None, help="Valgfritt: BK-felt på bruer (ellers autodetekteres)")

    args = ap.parse_args()

    arcpy.env.overwriteOutput = True

    # Inputs
    fwd_fc = os.path.join(args.fwd_gpkg, args.fwd_layer)
    if not arcpy.Exists(fwd_fc):
        # Noen ganger må gpkg adresseres slik: path.gpkg\\layer
        fwd_fc = args.fwd_gpkg + "\\" + args.fwd_layer
    if not arcpy.Exists(fwd_fc):
        raise FileNotFoundError(f"Fant ikke FWD-layer i GPKG: {args.fwd_gpkg} / {args.fwd_layer}")

    vegnett_in = fc_path(args.nvdb_gdb, args.vegnett_fc)
    bruer_in = fc_path(args.nvdb_gdb, args.bruer_fc)

    out_gdb = ensure_gdb(args.out_gdb)
    out_vegnett = os.path.join(out_gdb, args.out_vegnett_name)
    out_avvik = os.path.join(out_gdb, args.out_fwd_avvik_name)

    # Autodetekter BK-felt
    vegnett_bk_field = args.vegnett_bk_field or pick_first_field(vegnett_in, ["BK_VERDI", "BK", "Bruksklasse", "BRUKSKLASSE"])
    bru_bk_field = args.bru_bk_field or pick_first_field(bruer_in, ["BK_VERDI", "BK", "Bruksklasse", "BRUKSKLASSE", "TillattBK"])

    if vegnett_bk_field is None:
        log("ADVARSEL: Fant ikke BK-felt på vegnett (BK_VERDI/BK/Bruksklasse). Fortsetter uten veg-BK i vurderingen.")
    if bru_bk_field is None:
        log("ADVARSEL: Fant ikke BK-felt på bruer. Bru-stop vil da ikke fanges maskinelt (kun FWD).")

    # Kopier vegnett til output
    if arcpy.Exists(out_vegnett):
        arcpy.management.Delete(out_vegnett)
    log("Kopierer vegnett til output...")
    arcpy.management.CopyFeatures(vegnett_in, out_vegnett)

    # Bygg mapping FWD->Vegnett via near table
    scratch_gdb = arcpy.env.scratchGDB
    near_fwd_tbl = os.path.join(scratch_gdb, "near_fwd_to_vegnett")
    map_fwd_to_line = build_near_mapping(fwd_fc, out_vegnett, args.search_radius, near_fwd_tbl)

    # Aggreger FWD stats pr vegnett
    log("Aggregerer FWD-statistikk pr vegsegment...")
    fwd_stats_by_line, point_value = aggregate_fwd_by_vegnett(fwd_fc, args.fwd_value_field, map_fwd_to_line)

    # Persentil + under terskel må beregnes pr linje
    # (vi trenger verdilister; les på nytt, men effektivt via dict line->list)
    line_vals = {}
    for p_oid, v in point_value.items():
        line_oid = map_fwd_to_line.get(p_oid)
        if line_oid is None:
            continue
        line_vals.setdefault(line_oid, []).append(v)

    for line_oid, vals in line_vals.items():
        fwd_stats_by_line.setdefault(line_oid, {})
        fwd_stats_by_line[line_oid]["pctl"] = percentile_safe(vals, args.percentile)
        fwd_stats_by_line[line_oid]["under_thr"] = count_under(vals, args.threshold)

    # Bruer -> vegnett mapping
    near_bru_tbl = os.path.join(scratch_gdb, "near_bru_to_vegnett")
    map_bru_to_line = build_near_mapping(bruer_in, out_vegnett, args.search_radius, near_bru_tbl)
    bru_minbk_by_line = aggregate_bru_minbk_by_vegnett(bruer_in, bru_bk_field, map_bru_to_line)

    # Legg til felt i out_vegnett
    safe_add_field(out_vegnett, "FWD_N", "LONG")
    safe_add_field(out_vegnett, "FWD_MIN", "DOUBLE")
    safe_add_field(out_vegnett, "FWD_MAX", "DOUBLE")
    safe_add_field(out_vegnett, "FWD_MEAN", "DOUBLE")
    safe_add_field(out_vegnett, "FWD_STD", "DOUBLE")
    safe_add_field(out_vegnett, f"FWD_P{int(args.percentile)}", "DOUBLE")
    safe_add_field(out_vegnett, "FWD_U_THR", "LONG")  # under threshold count
    safe_add_field(out_vegnett, "BRU_MIN_BK", "LONG")
    safe_add_field(out_vegnett, "BK_FORESL", "LONG")  # foreslått BK (60 eller null)
    safe_add_field(out_vegnett, "STATUS", "TEXT", length=24)

    oid_line = arcpy.Describe(out_vegnett).OIDFieldName

    # Oppdater vegnett med stats + status
    log("Skriver felter og status på vegnett...")
    status_counts = {"OK": 0, "STOPP_BRU": 0, "STOPP_FWD": 0, "MANGLER_DATA": 0}

    pctl_field = f"FWD_P{int(args.percentile)}"

    upd_fields = [oid_line, "FWD_N", "FWD_MIN", "FWD_MAX", "FWD_MEAN", "FWD_STD", pctl_field, "FWD_U_THR", "BRU_MIN_BK", "BK_FORESL", "STATUS"]
    if vegnett_bk_field:
        # bare for evt. CSV senere; ikke nødvendig i update cursor
        pass

    with arcpy.da.UpdateCursor(out_vegnett, upd_fields) as cur:
        for row in cur:
            line_oid = int(row[0])

            st = fwd_stats_by_line.get(line_oid)
            if st:
                n = int(st.get("n", 0) or 0)
                row[1] = n
                row[2] = st.get("min")
                row[3] = st.get("max")
                row[4] = st.get("mean")
                row[5] = st.get("std")
                row[6] = st.get("pctl")
                row[7] = int(st.get("under_thr", 0) or 0)
            else:
                n = 0
                row[1] = 0
                row[2] = None
                row[3] = None
                row[4] = None
                row[5] = None
                row[6] = None
                row[7] = 0

            bru_minbk = bru_minbk_by_line.get(line_oid)
            row[8] = bru_minbk if bru_minbk is not None else None

            # Statuslogikk
            if n < args.min_fwd_n:
                status = "MANGLER_DATA"
                foresl = None
            else:
                # Bru er hard stop hvis tilgjengelig
                if bru_minbk is not None and bru_minbk < 60:
                    status = "STOPP_BRU"
                    foresl = None
                else:
                    pctl = row[6]
                    if pctl is None or float(pctl) < float(args.threshold):
                        status = "STOPP_FWD"
                        foresl = None
                    else:
                        status = "OK"
                        foresl = 60

            row[9] = foresl
            row[10] = status
            status_counts[status] = status_counts.get(status, 0) + 1

            cur.updateRow(row)

    log(f"STATUS opptelling: {status_counts}")

    # Avvikslag: kopier FWD-punkter under terskel, og legg inn vegnett_oid + verdi
    if arcpy.Exists(out_avvik):
        arcpy.management.Delete(out_avvik)

    log("Lager avvikslag (FWD under terskel)...")
    fwd_oid = arcpy.Describe(fwd_fc).OIDFieldName

    # Lag midlertidig lag/visning
    fwd_layer = "fwd_lyr_tmp"
    if arcpy.Exists(fwd_layer):
        arcpy.management.Delete(fwd_layer)
    arcpy.management.MakeFeatureLayer(fwd_fc, fwd_layer)

    # SQL varierer litt; bruk AddFieldDelimiters
    fld = arcpy.AddFieldDelimiters(fwd_layer, args.fwd_value_field)
    arcpy.management.SelectLayerByAttribute(fwd_layer, "NEW_SELECTION", f"{fld} < {args.threshold}")

    arcpy.management.CopyFeatures(fwd_layer, out_avvik)

    # Legg til koblingsfelt på avvikspunkter
    safe_add_field(out_avvik, "VEGNETT_OID", "LONG")
    safe_add_field(out_avvik, "FWD_TONN", "DOUBLE")

    map_avvik_to_line = build_near_mapping(out_avvik, out_vegnett, args.search_radius, os.path.join(scratch_gdb, "near_avvik_to_vegnett"))

    with arcpy.da.UpdateCursor(out_avvik, [arcpy.Describe(out_avvik).OIDFieldName, "VEGNETT_OID", "FWD_TONN", args.fwd_value_field]) as cur:
        for oid, _, _, raw in cur:
            oid = int(oid)
            cur.updateRow((oid, map_avvik_to_line.get(oid), to_float(raw), raw))

    # Valgfri CSV
    if args.csv_summary:
        log(f"Skriver CSV-sammendrag: {args.csv_summary}")
        # Minimal, robust CSV: summering per STATUS + generelt
        # (Du kan utvide til vegnummer/kontrakt osv når feltnavn er stabile i ditt vegnett)
        total = sum(status_counts.values())
        with open(args.csv_summary, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["Dato", datetime.date.today().isoformat()])
            w.writerow(["Terskel_tonn", args.threshold])
            w.writerow(["Persentil", args.percentile])
            w.writerow(["Min_FWD_N", args.min_fwd_n])
            w.writerow([])
            w.writerow(["STATUS", "Antall", "Andel_%"])
            for k in ["OK", "STOPP_BRU", "STOPP_FWD", "MANGLER_DATA"]:
                n = status_counts.get(k, 0)
                pct = (100.0 * n / total) if total else 0.0
                w.writerow([k, n, round(pct, 2)])

    log("FERDIG.")
    log(f"Output vegnett: {out_vegnett}")
    log(f"Output avvik:   {out_avvik}")

    # Kort “pkt 8”-konklusjon, direkte i logg (kan også skrives til rapport senere)
    log("Konklusjon:")
    log("Ja: du kan kjøre en fylkesvis screening for BKT8/50 → BKT8/60 basert på FWD + BK/bruer fra NVDB.")
    log("Dette gir et etterprøvbart, kartbart beslutningsgrunnlag, men er indikativt og må suppleres med kontroll av øvrige begrensninger der det er relevant.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
