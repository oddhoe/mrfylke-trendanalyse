# -*- coding: utf-8 -*-
"""
NVDB FWD / Nedbøyningsmåling (592) -> rapport per FV (MR), med tidsfilter 2017 -> i dag.

- Kun fylke=15 (Møre og Romsdal)
- Kun FV (vegkategori F)
- Kun målinger med måledato innenfor [2017-01-01, i dag]
- Rapport per FV i Markdown (+ valgfri CSV)

Merk:
- NVDB API Les V4 krever header X-Client (fra 2026).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import requests

NVDB_V4_BASE = "https://nvdbapiles.atlas.vegvesen.no/vegobjekter/api/v4"
OBJTYPE_NEDBOYNING = 592
DEFAULT_FYLKE_MR = 15  # Møre og Romsdal

CAPACITY_NAME_CANDIDATES = [
    "Bæreevne, temperaturkorrigert",
    "Bæreevne temperaturkorrigert",
    "Bæreevne (temperaturkorrigert)",
    "Bæreevne",
]

MEASURE_DATE_NAME_CANDIDATES = [
    "Måledato",
    "måledato",
    "Dato for måling",
    "Måledato (dato)",
]

DEVIATION_FIELD_HINTS = [
    "Avvik",
    "Svakhet",
    "Kommentar",
    "Merknad",
    "Kvalitet",
]


def date_to_str(d: Optional[dt.date]) -> str:
    return d.isoformat() if isinstance(d, dt.date) else ""


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None
    return None


def parse_date(x: Any) -> Optional[dt.date]:
    if x is None:
        return None
    if isinstance(x, dt.date) and not isinstance(x, dt.datetime):
        return x
    s = str(x).strip()
    if not s:
        return None

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return None

    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def mean(values: List[float]) -> float:
    return sum(values) / len(values)


def std(values: List[float], sample: bool = True) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mu = mean(values)
    denom = (n - 1) if sample else n
    var = sum((v - mu) ** 2 for v in values) / denom
    return math.sqrt(var)


def wkt_point_to_lonlat(wkt: str) -> Tuple[Optional[float], Optional[float]]:
    m = re.search(r"POINT(?: Z)?\s*\(\s*([-\d\.]+)\s+([-\d\.]+)", wkt or "")
    if not m:
        return None, None
    x = safe_float(m.group(1))
    y = safe_float(m.group(2))
    return x, y  # lon, lat for SRID=4326


def extract_egenskaper_map(obj: Dict[str, Any]) -> Dict[str, Any]:
    emap: Dict[str, Any] = {}
    for e in obj.get("egenskaper") or []:
        navn = e.get("navn")
        if not navn:
            continue
        if "verdi" in e:
            emap[navn] = e.get("verdi")
        elif "verdiTekst" in e:
            emap[navn] = e.get("verdiTekst")
        elif "verdiEnum" in e:
            emap[navn] = e.get("verdiEnum")
        else:
            emap[navn] = e
    return emap


def pick_capacity_tons(emap: Dict[str, Any]) -> Optional[float]:
    for k in CAPACITY_NAME_CANDIDATES:
        if k in emap:
            v = safe_float(emap.get(k))
            if v is not None:
                return v

    for key, val in emap.items():
        if isinstance(key, str) and "bæreevne" in key.lower():
            v = safe_float(val)
            if v is not None:
                return v
    return None


def pick_measure_date(emap: Dict[str, Any]) -> Optional[dt.date]:
    for k in MEASURE_DATE_NAME_CANDIDATES:
        if k in emap:
            d = parse_date(emap.get(k))
            if d:
                return d

    for key, val in emap.items():
        if isinstance(key, str) and "måledato" in key.lower():
            d = parse_date(val)
            if d:
                return d
    return None


def extract_vegsystem_refs(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    lok = obj.get("lokasjon") or {}
    return lok.get("vegsystemreferanser") or []


def fv_key_from_vegsystemref(vsr: Dict[str, Any]) -> Optional[str]:
    vs = vsr.get("vegsystem") or {}
    vegkategori = vs.get("vegkategori")
    nummer = vs.get("nummer")
    if vegkategori == "F" and isinstance(nummer, int):
        return f"FV{nummer}"
    return None


def extract_meter_from_vegsystemref(vsr: Dict[str, Any]) -> Optional[float]:
    strek = vsr.get("strekning") or {}
    return safe_float(strek.get("meter"))


def detect_other_deviation_reasons(emap: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    for key, val in emap.items():
        if not isinstance(key, str):
            continue
        if not any(h.lower() in key.lower() for h in DEVIATION_FIELD_HINTS):
            continue
        if val is None:
            continue
        if isinstance(val, str) and not val.strip():
            continue

        sval = str(val).strip()
        if len(sval) > 80:
            sval = sval[:77] + "..."
        reasons.append(f"{key}={sval}")
    return reasons


def nvdb_get_all_592(
    *,
    fylke: int,
    srid: int,
    antall: int,
    x_client: str,
    timeout: int = 60,
) -> List[Dict[str, Any]]:
    url = f"{NVDB_V4_BASE}/vegobjekter/{OBJTYPE_NEDBOYNING}"
    params = {
        "fylke": fylke,
        "srid": srid,
        "antall": antall,
        "inkluderAntall": "false",
        "inkluder": "egenskaper,lokasjon,geometri,metadata",
    }
    headers = {"X-Client": x_client}

    out: List[Dict[str, Any]] = []
    start: Optional[str] = None

    while True:
        p = dict(params)
        if start:
            p["start"] = start

        r = requests.get(url, params=p, headers=headers, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        out.extend(data.get("objekter") or [])

        meta = data.get("metadata") or {}
        nxt = meta.get("neste") or {}
        start = nxt.get("start")
        if not start:
            break

    return out


def build_rows(objs: List[Dict[str, Any]], srid_request: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for o in objs:
        emap = extract_egenskaper_map(o)
        tons = pick_capacity_tons(emap)
        mdate = pick_measure_date(emap)

        fv = None
        meter = None
        for vsr in extract_vegsystem_refs(o):
            k = fv_key_from_vegsystemref(vsr)
            if k:
                fv = k
                meter = extract_meter_from_vegsystemref(vsr)
                break

        geo = o.get("geometri") or {}
        wkt = geo.get("wkt") or ""
        srid_obj = geo.get("srid")

        lat = None
        lon = None
        if srid_request == 4326 and srid_obj == 4326:
            lon, lat = wkt_point_to_lonlat(wkt)

        reasons = detect_other_deviation_reasons(emap)

        rows.append(
            {
                "fv": fv,
                "id": o.get("id"),
                "tons": tons,
                "measure_date": mdate,
                "lat": lat,
                "lon": lon,
                "meter": meter,
                "srid": srid_obj,
                "wkt": wkt,
                "deviation_reasons": reasons,
            }
        )
    return rows


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = [
        "fv",
        "id",
        "tons",
        "measure_date",
        "lat",
        "lon",
        "meter",
        "srid",
        "wkt",
        "deviation_reasons",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        for r in rows:
            rr = dict(r)
            rr["measure_date"] = date_to_str(rr.get("measure_date"))
            rr["deviation_reasons"] = "|".join(rr.get("deviation_reasons") or [])
            w.writerow(rr)


def format_report_markdown(
    *,
    fv: str,
    rows: List[Dict[str, Any]],
    author: str,
    report_date: str,
    threshold_tons: float,
    target_text: str,
    date_from: dt.date,
    date_to: dt.date,
) -> str:
    vals = [float(r["tons"]) for r in rows if r.get("tons") is not None]
    if not vals:
        period = f"{date_from.isoformat()} – {date_to.isoformat()}"
        return (
            f"For hele {fv}:\n\n"
            f"Basert på FWD målinger ({period})\n\n"
            f"Vurdering av administrativ oppskriving: {target_text}\n"
            f"Dato: {report_date}\n"
            f"Utarbeidet av: {author}\n\n"
            "Sammendrag og konklusjon\n"
            "Ingen målinger med gjenkjennbar bæreevneverdi (tonn) innenfor dato-filteret.\n"
        )

    n = len(vals)
    mu = mean(vals)
    sd = std(vals, sample=True)
    mn = min(vals)
    mx = max(vals)
    low = mu - sd
    high = mu + sd

    under = [
        r for r in rows if r.get("tons") is not None and float(r["tons"]) < threshold_tons
    ]
    other_dev = [
        r
        for r in rows
        if (r.get("tons") is None or float(r["tons"]) >= threshold_tons)
        and (r.get("deviation_reasons") or [])
    ]

    deviation_total = len(under) + len(other_dev)
    under_pct = (len(under) / n) * 100.0 if n else 0.0

    dev_rows = [*under, *other_dev]
    dev_vals = [float(r["tons"]) for r in dev_rows if r.get("tons") is not None]
    dev_mu = mean(dev_vals) if dev_vals else float("nan")
    dev_sd = std(dev_vals, sample=True) if len(dev_vals) >= 2 else 0.0

    under_sorted = sorted(
        under, key=lambda r: (float(r["tons"]), r.get("meter") or 1e18)
    )

    def fmt(x: Any, nd: int = 2) -> str:
        if x is None:
            return ""
        if isinstance(x, float):
            return f"{x:.{nd}f}"
        if isinstance(x, int):
            return str(x)
        return str(x)

    period = f"{date_from.isoformat()} – {date_to.isoformat()}"
    lines: List[str] = []

    lines.append(f"For hele {fv}:\n")
    lines.append(f"Basert på FWD målinger ({period})\n")
    lines.append(f"Vurdering av administrativ oppskriving: {target_text}")
    lines.append(f"Dato: {report_date}")
    lines.append(f"Utarbeidet av: {author}\n")

    lines.append("Sammendrag og konklusjon")
    lines.append(
        f"Totalt {n} målinger. Snitt bæreevne {mu:.1f} t. ±1 SD gir intervallet "
        f"{low:.1f}–{high:.1f} t."
    )
    lines.append(
        f"Avvikslaget inneholder {deviation_total} punkter totalt. Av disse er "
        f"kun {len(under)} (≈{under_pct:.2f}%) under {threshold_tons:g} tonn, "
        f"mens {len(other_dev)} punkter har bæreevne over {threshold_tons:g} tonn "
        "men ligger i Avvik av andre årsaker."
    )
    lines.append(
        "Det faglige beslutningsgrunnlaget blir: Vegens faktiske bæreevne støtter "
        "administrativ oppskriving til BKT10/60 under forutsetning av at bruer har "
        "samme BK og punktene under undersøkes før oppgradering skjer."
    )
    lines.append("")

    lines.append(f"Punkter under {threshold_tons:g} tonn")
    lines.append("")
    lines.append("| # | Bæreevne | lat | lon | Meter | Måledato |")
    lines.append("|---:|---:|---:|---:|---:|---:|")
    for i, r in enumerate(under_sorted, 1):
        md = date_to_str(r.get("measure_date"))
        lines.append(
            f"| {i} | {fmt(float(r['tons']), 2)} | {fmt(r.get('lat'), 2)} | "
            f"{fmt(r.get('lon'), 2)} | {fmt(r.get('meter'), 1)} | {md} |"
        )
    lines.append("")

    lines.append("Datagrunnlag og tolkning")
    lines.append("")
    lines.append("| Parameter | Verdi |")
    lines.append("|---|---:|")
    lines.append(f"| Antall målinger (total) | {n} |")
    lines.append(f"| Gjennomsnitt (tonn) | {mu:.3f} |")
    lines.append(f"| Standardavvik (tonn) | {sd:.3f} |")
    lines.append(f"| Minimum (tonn) | {mn:.1f} |")
    lines.append(f"| Maksimum (tonn) | {mx:.1f} |")
    lines.append(f"| Antall avvikspunkter (<{threshold_tons:g} t) | {len(under)} |")
    lines.append(f"| Andel av total (%) | {under_pct:.2f} |")
    lines.append(f"| Snitt i avvikspunkter (tonn) | {dev_mu:.3f} |")
    lines.append(f"| Standardavvik avvikspunkter (tonn) | {dev_sd:.3f} |")

    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fylke", type=int, default=DEFAULT_FYLKE_MR)
    ap.add_argument("--srid", type=int, default=4326)
    ap.add_argument("--antall", type=int, default=1000)
    ap.add_argument("--x-client", required=True)
    ap.add_argument("--threshold", type=float, default=10.0)
    ap.add_argument("--outdir", default="nvdb_fwd_rapporter_mr")
    ap.add_argument("--author", default="Odd Erling Hoem / Avdelingsingeniør")
    ap.add_argument("--date", default=None)
    ap.add_argument("--write-csv", action="store_true")
    ap.add_argument("--year-from", type=int, default=2017)
    ap.add_argument("--target-text", default="FV → BKT10/50 → BKT10/60")
    args = ap.parse_args()

    today = dt.date.today()
    date_from = dt.date(args.year_from, 1, 1)
    date_to = today
    report_date = args.date or today.strftime("%d.%m.%Y")

    os.makedirs(args.outdir, exist_ok=True)

    all_objs = nvdb_get_all_592(
        fylke=args.fylke,
        srid=args.srid,
        antall=args.antall,
        x_client=args.x_client,
    )

    rows_all = build_rows(all_objs, srid_request=args.srid)

    rows: List[Dict[str, Any]] = []
    for r in rows_all:
        if not r.get("fv"):
            continue
        md = r.get("measure_date")
        if not isinstance(md, dt.date):
            continue
        if md < date_from or md > date_to:
            continue
        rows.append(r)

    by_fv: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_fv.setdefault(r["fv"], []).append(r)

    if not by_fv:
        raise SystemExit(
            "Fant ingen FV-målinger i MR innenfor dato-filteret (2017->i dag)."
        )

    def sort_key(fv: str) -> int:
        return int(re.sub(r"\D", "", fv) or "0")

    for fv, rws in sorted(by_fv.items(), key=lambda x: sort_key(x[0])):
        md_text = format_report_markdown(
            fv=fv,
            rows=rws,
            author=args.author,
            report_date=report_date,
            threshold_tons=args.threshold,
            target_text=args.target_text.replace("FV", fv),
            date_from=date_from,
            date_to=date_to,
        )
        md_path = os.path.join(
            args.outdir, f"{fv}_bkt50_til_60_fwd_2017_{date_to.year}.md"
        )
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md_text)

        if args.write_csv:
            csv_path = os.path.join(
                args.outdir, f"{fv}_nedboyning_592_2017_{date_to.year}.csv"
            )
            write_csv(csv_path, rws)

    print(f"Skrev {len(by_fv)} rapport(er) til: {os.path.abspath(args.outdir)}")


if __name__ == "__main__":
    main()
