# -*- coding: utf-8 -*-
"""
adm_screening_fv.py  (v5 - lineært referert)

Fylkesvis screening: BKT8/50 -> BKT8/60 (administrativ oppskriving) – INDIKATIV

Dette er v5 som fjerner "nearest geometry"-problemet i kryss ved å bruke lineær referanse (LRS):
  1) Bygger ruter (routes) av vegnett med VEGLENKESEKV_ID + STARTPOS/SLUTTPOS (TWO_FIELDS)
  2) Lokaliserer FWD-punkter langs ruter (LocateFeaturesAlongRoutes) -> får measure (M)
  3) Aggregerer FWD per faste bins (split_m) langs rute (f.eks. 500 m)
  4) Knytter bruer via pos-overlapp i measure (STARTPOS/SLUTTPOS) mot samme bins
  5) Lager line-event FC (polylinjer) med status OK/STOPP_BRU/STOPP_FWD/MANGLER_DATA
  6) Rapporterer både antall og lengde (km)

Fordel:
 - Stabil dekning der FWD-kartet er kontinuerlig, også i kryss og tett nett.

Kjøring (PowerShell eksempel):
& "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" `
  "D:\...\adm_screening_fv.py" `
  --fwd_gpkg "D:\...\FWD_592_MR.gpkg" `
  --fwd_layer "FWD_592_MR_2017_2026" `
  --fwd_value_field "tons" `
  --nvdb_gdb "D:\...\nvdb_radata.gdb" `
  --vegnett_fc "Vegnett" `
  --bruer_fc "Bruer" `
  --bru_tonn_field "TILLATT_TONN" `
  --out_gdb "D:\...\Screening_BKT8_60.gdb" `
  --threshold 10.0 --percentile 95 --min_fwd_n 3 --search_radius 50 `
  --split_m 500 `
  --csv_summary "D:\...\screening_summary.csv"

MERKNAD:
Dette er en screening (indikativ). Resultatet bør brukes som kartbart beslutningsgrunnlag,
og suppleres med vurdering av øvrige begrensninger der relevant.
"""

import os
import math
import csv
import argparse
import datetime
from collections import defaultdict

import numpy as np

try:
    import arcpy
except Exception:
    print("FEIL: arcpy er ikke tilgjengelig. Kjør i ArcGIS Pro Python (arcgispro-py3).")
    raise


# -----------------------------
# Logging
# -----------------------------
def log(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts}  {msg}")


# -----------------------------
# Helpers
# -----------------------------
def ensure_gdb(path):
    folder = os.path.dirname(path)
    name = os.path.basename(path)
    if folder and not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True)
    if not arcpy.Exists(path):
        log(f"Oppretter GDB: {path}")
        arcpy.management.CreateFileGDB(folder, name)
    return path


def list_fields(fc):
    return [f.name for f in arcpy.ListFields(fc)]


def pick_first_field(fc, candidates):
    fields = set(list_fields(fc))
    for c in candidates:
        if c in fields:
            return c
    low = {f.lower(): f for f in list_fields(fc)}
    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]
    return None


def safe_add_field(fc, name, ftype, length=None):
    if name in set(list_fields(fc)):
        return
    if length and ftype.upper() in ("TEXT", "STRING"):
        arcpy.management.AddField(fc, name, ftype, field_length=length)
    else:
        arcpy.management.AddField(fc, name, ftype)


def to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def percentile_safe(vals, p):
    if not vals:
        return None
    return float(np.percentile(np.array(vals, dtype=float), p))


def mean_safe(vals):
    if not vals:
        return None
    return float(np.mean(np.array(vals, dtype=float)))


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
    return int(np.sum(np.array(vals, dtype=float) < float(thr)))


def compute_stats(vals, percentile, threshold):
    if not vals:
        return None
    arr = np.array(vals, dtype=float)
    return {
        "n": int(len(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "mean": float(np.mean(arr)),
        "std": float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0,
        "pctl": float(np.percentile(arr, percentile)),
        "under_thr": int(np.sum(arr < float(threshold))),
    }


def maybe_project_to_match(in_fc, target_sr, scratch_name):
    in_sr = arcpy.Describe(in_fc).spatialReference
    if in_sr is None or target_sr is None:
        log("ADVARSEL: Fant ikke spatial reference på ett av lagene. Fortsetter uten reprojeksjon.")
        return in_fc

    in_code = getattr(in_sr, "factoryCode", None)
    tgt_code = getattr(target_sr, "factoryCode", None)

    if in_code and tgt_code and in_code == tgt_code:
        return in_fc

    out_fc = os.path.join(arcpy.env.scratchGDB, scratch_name)
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)

    log(f"Projiserer {os.path.basename(in_fc)} til rutelagets koordinatsystem...")
    arcpy.management.Project(in_fc, out_fc, target_sr)
    return out_fc


def copy_filtered(in_fc, out_fc, where_sql=None):
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)
    if where_sql:
        log(f"Filtrerer vegnett med WHERE: {where_sql}")
        lyr = "tmp_lyr_v5"
        if arcpy.Exists(lyr):
            arcpy.management.Delete(lyr)
        arcpy.management.MakeFeatureLayer(in_fc, lyr, where_sql)
        arcpy.management.CopyFeatures(lyr, out_fc)
        arcpy.management.Delete(lyr)
    else:
        arcpy.management.CopyFeatures(in_fc, out_fc)


def fc_path(gdb, name_or_path):
    if arcpy.Exists(name_or_path):
        return name_or_path
    p = os.path.join(gdb, name_or_path)
    if arcpy.Exists(p):
        return p
    raise FileNotFoundError(f"Fant ikke featureclass/layer: {name_or_path}")


# -----------------------------
# LRS / Routes
# -----------------------------
def create_routes_from_vegnett(vegnett_fc, out_routes_fc, route_id_field="VEGLENKESEKV_ID",
                               from_field="STARTPOS", to_field="SLUTTPOS"):
    # CreateRoutes må få "measure source" TWO_FIELDS
    # Input kan ha mange features per route_id; dette er OK hvis from/to er konsistente.
    if arcpy.Exists(out_routes_fc):
        arcpy.management.Delete(out_routes_fc)

    for f in (route_id_field, from_field, to_field):
        if f not in set(list_fields(vegnett_fc)):
            raise ValueError(f"Mangler felt {f} i vegnett. Kan ikke lage ruter (CreateRoutes TWO_FIELDS).")

    log("Bygger ruter (CreateRoutes, TWO_FIELDS) ...")
    # coordinate_priority, measure_factor etc. trengs ikke her
    arcpy.lr.CreateRoutes(
        in_line_features=vegnett_fc,
        route_id_field=route_id_field,
        out_feature_class=out_routes_fc,
        measure_source="TWO_FIELDS",
        from_measure_field=from_field,
        to_measure_field=to_field,
        coordinate_priority="UPPER_LEFT",
        measure_factor=1.0,
        measure_offset=0.0,
        ignore_gaps="IGNORE"
    )
    return out_routes_fc


def locate_points_along_routes(points_fc, routes_fc, route_id_field="VEGLENKESEKV_ID",
                               search_radius_m=50.0, out_table=None):
    if out_table is None:
        out_table = os.path.join(arcpy.env.scratchGDB, "fwd_events_tbl_v5")
    if arcpy.Exists(out_table):
        arcpy.management.Delete(out_table)

    rad = f"{float(search_radius_m)} Meters"
    props = f"{route_id_field} POINT MEAS"

    log("Lokaliserer FWD-punkter langs ruter (Locate Features Along Routes)...")

    # GP-signatur (som ArcGIS Pro forventer):
    # in_features, in_routes, route_id_field, search_radius,
    # out_table, out_event_properties,
    # route_locations, distance (DISTANCE|NO_DISTANCE),
    # zero_length_events, fields (FIELDS|NO_FIELDS)
    arcpy.lr.LocateFeaturesAlongRoutes(
        points_fc,
        routes_fc,
        route_id_field,
        rad,
        out_table,
        props,
        "FIRST",
        "DISTANCE",   # << dette er det feilen din klager på
        "ZERO",
        "FIELDS"
    )
    return out_table


# -----------------------------
# Binning + Bru overlap
# -----------------------------
def bin_measure(meas, bin_size):
    # bin start på 0, bin_end = start + bin_size
    b0 = math.floor(float(meas) / float(bin_size)) * float(bin_size)
    return b0, b0 + float(bin_size)


def build_bru_index(bruer_fc, bru_tonn_field, route_id_field="VEGLENKESEKV_ID",
                    from_field="STARTPOS", to_field="SLUTTPOS",
                    valid_route_ids=None):
    # return: dict rid -> sorted list of (sp, ep, tonn)
    if bru_tonn_field is None:
        return {}, 0, 0

    need = [route_id_field, from_field, to_field, bru_tonn_field]
    for f in need:
        if f not in set(list_fields(bruer_fc)):
            raise ValueError(f"Mangler felt {f} i bruer. Kan ikke bruke pos-overlapp.")

    bruer_by_rid = defaultdict(list)
    n_tot = 0
    n_lt60 = 0

    with arcpy.da.SearchCursor(bruer_fc, [route_id_field, from_field, to_field, bru_tonn_field]) as cur:
        for rid, sp, ep, tonn in cur:
            if rid is None or sp is None or ep is None:
                continue
            if valid_route_ids is not None and int(rid) not in valid_route_ids:
                continue
            t = to_float(tonn)
            if t is None:
                continue
            n_tot += 1
            it = int(round(t))
            if it < 60:
                n_lt60 += 1
            s = float(sp); e = float(ep)
            if e < s:
                s, e = e, s
            bruer_by_rid[int(rid)].append((s, e, it))

    for rid in list(bruer_by_rid.keys()):
        bruer_by_rid[rid].sort(key=lambda x: x[0])

    return dict(bruer_by_rid), n_lt60, n_tot


def min_bru_tonn_for_bin(bruer_list, b_start, b_end):
    # bruer_list sortert på startpos
    # finn min tonn der (bru_sp <= b_end and bru_ep >= b_start)
    if not bruer_list:
        return None
    mn = None
    # enkel scan – dette er raskt nok i praksis for fylkesvis (få bruer per rid)
    for sp, ep, t in bruer_list:
        if sp > b_end:
            break
        if ep >= b_start and sp <= b_end:
            mn = t if mn is None else min(mn, t)
    return mn


# -----------------------------
# Event-line output
# -----------------------------
def create_bin_event_table(out_table, route_id_field="VEGLENKESEKV_ID"):
    # Lager en "event table" som MakeRouteEventLayer kan lese: RID, FROM_M, TO_M + attributter
    if arcpy.Exists(out_table):
        arcpy.management.Delete(out_table)

    gdb = os.path.dirname(out_table)
    name = os.path.basename(out_table)
    arcpy.management.CreateTable(gdb, name)

    arcpy.management.AddField(out_table, route_id_field, "LONG")
    arcpy.management.AddField(out_table, "FROM_M", "DOUBLE")
    arcpy.management.AddField(out_table, "TO_M", "DOUBLE")

    arcpy.management.AddField(out_table, "FWD_N", "LONG")
    arcpy.management.AddField(out_table, "FWD_MIN", "DOUBLE")
    arcpy.management.AddField(out_table, "FWD_MAX", "DOUBLE")
    arcpy.management.AddField(out_table, "FWD_MEAN", "DOUBLE")
    arcpy.management.AddField(out_table, "FWD_STD", "DOUBLE")
    arcpy.management.AddField(out_table, "FWD_PCTL", "DOUBLE")
    arcpy.management.AddField(out_table, "FWD_U_THR", "LONG")

    arcpy.management.AddField(out_table, "BRU_MIN_T", "LONG")
    arcpy.management.AddField(out_table, "TILLATT_FO", "LONG")
    arcpy.management.AddField(out_table, "STATUS", "TEXT", field_length=24)

    return out_table


def make_event_lines(routes_fc, event_table, out_fc, route_id_field="VEGLENKESEKV_ID"):
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)

    lyr = "route_event_lyr_v5"
    if arcpy.Exists(lyr):
        arcpy.management.Delete(lyr)

    # Event props: "<RID> LINE <FROM> <TO>"
    props = f"{route_id_field} LINE FROM_M TO_M"
    arcpy.lr.MakeRouteEventLayer(routes_fc, route_id_field, event_table, props, lyr)
    arcpy.management.CopyFeatures(lyr, out_fc)
    arcpy.management.Delete(lyr)
    return out_fc


def length_by_status(fc, status_field="STATUS"):
    fld_len = "Shape_Length" if "Shape_Length" in set(list_fields(fc)) else None
    fields = [status_field, fld_len] if fld_len else [status_field, "SHAPE@LENGTH"]

    sums = defaultdict(float)
    with arcpy.da.SearchCursor(fc, fields) as cur:
        for st, l in cur:
            if st is None or l is None:
                continue
            sums[str(st)] += float(l)
    return dict(sums)


def total_length(fc):
    fld_len = "Shape_Length" if "Shape_Length" in set(list_fields(fc)) else None
    s = 0.0
    with arcpy.da.SearchCursor(fc, [fld_len] if fld_len else ["SHAPE@LENGTH"]) as cur:
        for (l,) in cur:
            if l is None:
                continue
            s += float(l)
    return s


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="v5: Screening BKT8/50->BKT8/60 med lineær referanse (routes + measures).")
    ap.add_argument("--fwd_gpkg", required=True)
    ap.add_argument("--fwd_layer", required=True)
    ap.add_argument("--fwd_value_field", default=None)

    ap.add_argument("--nvdb_gdb", required=True)
    ap.add_argument("--vegnett_fc", required=True)
    ap.add_argument("--bruer_fc", required=True)
    ap.add_argument("--bru_tonn_field", default="TILLATT_TONN")

    ap.add_argument("--out_gdb", required=True)
    ap.add_argument("--out_name", default="BKT8_60_screening_LRS")
    ap.add_argument("--out_avvik_name", default="FWD_avvik_under_terskel")

    ap.add_argument("--threshold", type=float, default=10.0)
    ap.add_argument("--percentile", type=float, default=95.0)
    ap.add_argument("--min_fwd_n", type=int, default=3)
    ap.add_argument("--search_radius", type=float, default=50.0)
    ap.add_argument("--split_m", type=float, default=500.0)

    ap.add_argument("--where_vegnett", default="VEGKATEGORI = 'F'")
    ap.add_argument("--csv_summary", default=None)

    args = ap.parse_args()
    arcpy.env.overwriteOutput = True

    out_gdb = ensure_gdb(args.out_gdb)

    # Resolve inputs
    fwd_fc = os.path.join(args.fwd_gpkg, args.fwd_layer)
    if not arcpy.Exists(fwd_fc):
        # forsøk "workspace\\layer" variant
        fwd_fc = args.fwd_gpkg + "\\" + args.fwd_layer
    if not arcpy.Exists(fwd_fc):
        raise FileNotFoundError(f"Fant ikke FWD-layer i GPKG: {args.fwd_gpkg} / {args.fwd_layer}")

    vegnett_in = fc_path(args.nvdb_gdb, args.vegnett_fc)
    bruer_in = fc_path(args.nvdb_gdb, args.bruer_fc)

    # FWD value field
    if args.fwd_value_field:
        fwd_value_field = args.fwd_value_field
        if fwd_value_field not in set(list_fields(fwd_fc)):
            fwd_value_field = pick_first_field(fwd_fc, [args.fwd_value_field])
            if not fwd_value_field:
                raise ValueError(f"Fant ikke fwd_value_field '{args.fwd_value_field}' i {fwd_fc}.")
    else:
        fwd_value_field = pick_first_field(fwd_fc, ["BAREEVNE_TONN", "tons", "TONS", "BAREEVNE", "TONN"])
        if not fwd_value_field:
            raise ValueError("Fant ikke FWD-verdi-felt automatisk. Oppgi --fwd_value_field.")
    log(f"FWD-verdi felt: {fwd_value_field}")

    # Bru field
    bru_tonn_field = args.bru_tonn_field
    if bru_tonn_field not in set(list_fields(bruer_in)):
        bru_tonn_field2 = pick_first_field(bruer_in, [bru_tonn_field])
        if bru_tonn_field2:
            bru_tonn_field = bru_tonn_field2
        else:
            log(f"ADVARSEL: Fant ikke '{args.bru_tonn_field}' på bruer. Bru-stop vil ikke fanges.")
            bru_tonn_field = None
    if bru_tonn_field:
        log(f"Bru-begrensning (tonn) felt: {bru_tonn_field}")

    # Copy + filter vegnett til output GDB (for kontroll/etterprøvbarhet)
    vegnett_fv = os.path.join(out_gdb, "Vegnett_FV_tmp")
    copy_filtered(vegnett_in, vegnett_fv, (args.where_vegnett or "").strip())

    # Build routes in scratch (eller out_gdb)
    routes_fc = os.path.join(out_gdb, "Routes_VEGLENKESEKV")
    create_routes_from_vegnett(vegnett_fv, routes_fc,
                               route_id_field="VEGLENKESEKV_ID",
                               from_field="STARTPOS", to_field="SLUTTPOS")

    # Project FWD to match routes SR
    routes_sr = arcpy.Describe(routes_fc).spatialReference
    fwd_proj = maybe_project_to_match(fwd_fc, routes_sr, "fwd_proj_for_routes_v5")

    # Locate FWD along routes (event table with MEAS)
    events_tbl = os.path.join(arcpy.env.scratchGDB, "fwd_events_v5")
    events_tbl = locate_points_along_routes(
        points_fc=fwd_proj,
        routes_fc=routes_fc,
        route_id_field="VEGLENKESEKV_ID",
        search_radius_m=args.search_radius,
        out_table=events_tbl
    )

    # Build set of valid route ids (FV)
    valid_route_ids = set()
    with arcpy.da.SearchCursor(vegnett_fv, ["VEGLENKESEKV_ID"]) as cur:
        for (rid,) in cur:
            if rid is not None:
                valid_route_ids.add(int(rid))

    # Read located events and bin them
    # events_tbl has fields: VEGLENKESEKV_ID, MEAS, DIST_M, plus original point fields (in_fields="FIELDS")
    if "MEAS" not in set(list_fields(events_tbl)):
        raise RuntimeError("LocateFeaturesAlongRoutes ga ikke feltet MEAS. Sjekk route_id_field/out_event_properties.")

    vals_by_bin = defaultdict(list)

    # Vi trenger kobling til opprinnelig FWD-verdi: LocateFeaturesAlongRoutes tar med input-felter når in_fields="FIELDS"
    # Så fwd_value_field bør være tilgjengelig i events_tbl (evt med prefiks). Vi finner felt robust.
    evt_fields = set(list_fields(events_tbl))
    fwd_evt_field = fwd_value_field if fwd_value_field in evt_fields else pick_first_field(events_tbl, [fwd_value_field])
    if not fwd_evt_field:
        raise RuntimeError(f"Fant ikke FWD-verdi-felt i events-tabell: {fwd_value_field}. Tilgjengelige felt: {sorted(evt_fields)[:20]} ...")

    log(f"FWD-verdi i events-tabell: {fwd_evt_field}")
    log("Aggregerer FWD på bins langs rute...")

    with arcpy.da.SearchCursor(events_tbl, ["VEGLENKESEKV_ID", "MEAS", fwd_evt_field]) as cur:
        for rid, meas, v in cur:
            if rid is None or meas is None:
                continue
            rid = int(rid)
            if rid not in valid_route_ids:
                continue
            fv = to_float(v)
            if fv is None:
                continue
            b0, b1 = bin_measure(meas, args.split_m)
            vals_by_bin[(rid, b0, b1)].append(fv)

    # Build bru index by route id (FV-only)
    bruer_proj = maybe_project_to_match(bruer_in, routes_sr, "bruer_proj_for_routes_v5")
    bruer_by_rid, n_bru_lt60, n_bru_tot = build_bru_index(
        bruer_proj, bru_tonn_field,
        route_id_field="VEGLENKESEKV_ID",
        from_field="STARTPOS", to_field="SLUTTPOS",
        valid_route_ids=valid_route_ids
    )
    if bru_tonn_field:
        log(f"Bruer med tonn-verdi (FV): {n_bru_tot}. Bruer <60 tonn: {n_bru_lt60}.")

    # Determine all bins we want to output:
    #  - Bins with data (vals_by_bin keys)
    #  - PLUS bins that exist along FV routes but mangler data? (for dekning)
    #
    # For dekning må vi lage bins langs hver rute mellom min/max measure fra vegnett (STARTPOS/SLUTTPOS)
    # Vi kan hente min/max per route fra vegnett_fv.
    minmax_by_rid = {}
    with arcpy.da.SearchCursor(vegnett_fv, ["VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS"]) as cur:
        for rid, sp, ep in cur:
            if rid is None or sp is None or ep is None:
                continue
            rid = int(rid)
            s = float(sp); e = float(ep)
            if e < s:
                s, e = e, s
            mm = minmax_by_rid.get(rid)
            if mm is None:
                minmax_by_rid[rid] = [s, e]
            else:
                mm[0] = min(mm[0], s)
                mm[1] = max(mm[1], e)

    all_bins = set()
    bin_size = float(args.split_m)
    for rid, (mn, mx) in minmax_by_rid.items():
        # vi lager bins fra floor(mn/bin)*bin til ceil(mx/bin)*bin
        start = math.floor(mn / bin_size) * bin_size
        end = math.ceil(mx / bin_size) * bin_size
        x = start
        while x < end:
            all_bins.add((rid, x, x + bin_size))
            x += bin_size

    log(f"Antall rute-bins (total): {len(all_bins)}")
    log(f"Antall rute-bins med FWD-data: {len(vals_by_bin)}")

    # Create event table (bins)
    bins_tbl = os.path.join(out_gdb, "Bins_EventTable")
    create_bin_event_table(bins_tbl, route_id_field="VEGLENKESEKV_ID")

    status_counts = {"OK": 0, "STOPP_BRU": 0, "STOPP_FWD": 0, "MANGLER_DATA": 0}

    ins_fields = ["VEGLENKESEKV_ID", "FROM_M", "TO_M",
                  "FWD_N", "FWD_MIN", "FWD_MAX", "FWD_MEAN", "FWD_STD", "FWD_PCTL", "FWD_U_THR",
                  "BRU_MIN_T", "TILLATT_FO", "STATUS"]

    with arcpy.da.InsertCursor(bins_tbl, ins_fields) as ic:
        for rid, b0, b1 in sorted(all_bins):
            vals = vals_by_bin.get((rid, b0, b1), [])
            st = compute_stats(vals, args.percentile, args.threshold) if vals else None
            n = int(st["n"]) if st else 0

            # Bru overlap (measure)
            bru_min_t = None
            if bru_tonn_field:
                bru_min_t = min_bru_tonn_for_bin(bruer_by_rid.get(rid, []), b0, b1)

            # Statuslogikk
            if n < int(args.min_fwd_n):
                status = "MANGLER_DATA"
                foresl = None
            else:
                if bru_min_t is not None and int(bru_min_t) < 60:
                    status = "STOPP_BRU"
                    foresl = None
                else:
                    pctl = st["pctl"] if st else None
                    if pctl is None or float(pctl) < float(args.threshold):
                        status = "STOPP_FWD"
                        foresl = None
                    else:
                        status = "OK"
                        foresl = 60

            status_counts[status] += 1

            ic.insertRow((
                int(rid), float(b0), float(b1),
                n,
                st["min"] if st else None,
                st["max"] if st else None,
                st["mean"] if st else None,
                st["std"] if st else None,
                st["pctl"] if st else None,
                st["under_thr"] if st else 0,
                int(bru_min_t) if bru_min_t is not None else None,
                int(foresl) if foresl is not None else None,
                status
            ))

    total_bins = sum(status_counts.values())
    vurdert_bins = status_counts["OK"] + status_counts["STOPP_BRU"] + status_counts["STOPP_FWD"]
    dekning_bins = (100.0 * vurdert_bins / total_bins) if total_bins else 0.0
    ok_andel_vurdert = (100.0 * status_counts["OK"] / vurdert_bins) if vurdert_bins else 0.0

    log(f"STATUS opptelling (bins): {status_counts}")
    log(f"Dekning (bins): {vurdert_bins}/{total_bins} = {dekning_bins:.2f}%")
    log(f"OK-andel av vurderte (bins): {status_counts['OK']}/{vurdert_bins} = {ok_andel_vurdert:.2f}%")

    # Make line events feature class
    out_lines = os.path.join(out_gdb, args.out_name)
    make_event_lines(routes_fc, bins_tbl, out_lines, route_id_field="VEGLENKESEKV_ID")

    # Length reporting on event lines
    total_m = total_length(out_lines)
    by_len = length_by_status(out_lines, "STATUS")
    ok_m = by_len.get("OK", 0.0)
    bru_m = by_len.get("STOPP_BRU", 0.0)
    fwd_m = by_len.get("STOPP_FWD", 0.0)
    mang_m = by_len.get("MANGLER_DATA", 0.0)
    vurdert_m = ok_m + bru_m + fwd_m

    def km(x): return x / 1000.0

    log("Lengde (km) basert på line-event output:")
    log(f"  Total FV (i analyse): {km(total_m):.1f} km")
    log(f"  Vurdert:             {km(vurdert_m):.1f} km  ({(100.0*vurdert_m/total_m if total_m else 0):.2f}%)")
    log(f"   - OK:               {km(ok_m):.1f} km")
    log(f"   - STOPP_BRU:        {km(bru_m):.1f} km")
    log(f"   - STOPP_FWD:        {km(fwd_m):.1f} km")
    log(f"  MANGLER_DATA:        {km(mang_m):.1f} km")

    # Avvikspunkt (FWD under terskel) - behold i kart som før (geometri), men vi kan også skrive ut med RID/MEAS senere
    out_avvik = os.path.join(out_gdb, args.out_avvik_name)
    if arcpy.Exists(out_avvik):
        arcpy.management.Delete(out_avvik)

    log("Lager avvikspunkt (FWD under terskel)...")
    lyr = "fwd_lyr_avvik_v5"
    if arcpy.Exists(lyr):
        arcpy.management.Delete(lyr)
    arcpy.management.MakeFeatureLayer(fwd_proj, lyr)
    fld = arcpy.AddFieldDelimiters(lyr, fwd_value_field)
    arcpy.management.SelectLayerByAttribute(lyr, "NEW_SELECTION", f"{fld} < {float(args.threshold)}")
    arcpy.management.CopyFeatures(lyr, out_avvik)
    try:
        arcpy.management.Delete(lyr)
    except Exception:
        pass

    # CSV summary
    if args.csv_summary:
        log(f"Skriver CSV-sammendrag: {args.csv_summary}")
        with open(args.csv_summary, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["Dato", datetime.date.today().isoformat()])
            w.writerow(["Where_vegnett", (args.where_vegnett or "").strip()])
            w.writerow(["Split_m", args.split_m])
            w.writerow(["Search_radius_m", args.search_radius])
            w.writerow(["Terskel_FWD_tonn", args.threshold])
            w.writerow(["Persentil", args.percentile])
            w.writerow(["Min_FWD_N", args.min_fwd_n])
            w.writerow([])
            w.writerow(["STATUS", "Antall_bins", "Andel_bins_%"])
            for k in ["OK", "STOPP_BRU", "STOPP_FWD", "MANGLER_DATA"]:
                n = status_counts.get(k, 0)
                pct = (100.0 * n / total_bins) if total_bins else 0.0
                w.writerow([k, n, round(pct, 2)])
            w.writerow([])
            w.writerow(["Lengde_total_km", round(km(total_m), 3)])
            w.writerow(["Lengde_vurdert_km", round(km(vurdert_m), 3)])
            w.writerow(["Lengde_OK_km", round(km(ok_m), 3)])
            w.writerow(["Lengde_STOPP_BRU_km", round(km(bru_m), 3)])
            w.writerow(["Lengde_STOPP_FWD_km", round(km(fwd_m), 3)])
            w.writerow(["Lengde_MANGLER_DATA_km", round(km(mang_m), 3)])
            w.writerow([])
            w.writerow(["Bruer_med_tonnverdi_FV", n_bru_tot])
            w.writerow(["Bruer_under_60_FV", n_bru_lt60])

    log("FERDIG.")
    log(f"Output linje-event: {out_lines}")
    log(f"Output avvikspunkt: {out_avvik}")

    log("Konklusjon:")
    log("Ja: du kan kjøre en fylkesvis screening for BKT8/50 → BKT8/60 basert på FWD + bruer fra NVDB.")
    log("v5 bruker lineær referanse og unngår 'nearest geometry'-hull i kryss.")
    log("Dette gir et etterprøvbart, kartbart beslutningsgrunnlag, men er indikativt og må suppleres der relevant.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
