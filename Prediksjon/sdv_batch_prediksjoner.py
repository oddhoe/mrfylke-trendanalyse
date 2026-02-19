#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sdv_batch_prediksjoner.py
========================
Leser ViaPPS .sdv-filer fra ROTMAPPE/ÅR/*.sdv, normaliserer kolonnenavn på tvers av årganger,
bygger prediksjon per 20m-segment og skriver:

  1) GeoPackage: segmenter (punkter)  -> layer: "segmenter"
  2) CSV:        tilstand_serie       -> tabell til Dashboard/Experience Builder (målt + predikert)

Viktige egenskaper:
- IRI-variant støtte: "Alfred IRI", "IRI", "Class1 IRI" -> iri_mm_m
- Spor: "Spordybde" -> spor_mm
- Robust vegkode fra filnavn: FV65, FV065, FV00065, FV00539, FV06000 -> FV00065 osv.
- Strekning fra filnavn: S11D1 osv. (segmenter splittes per strekning)
- 20m-binning per strekning basert på "Utkjørt meter" (fallback "Fra vegmeter")
- Regresjon per segment med NaN-filter (sklearn tåler ikke NaN)
- Geometri: lat/lon hvis finnes, ellers UTM33 (Sone 33V Ø/N) -> WGS84

Avhengigheter:
  pip install pandas numpy geopandas shapely pyproj scikit-learn tqdm

Kjøring:
  python sdv_batch_prediksjoner.py
"""

from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from sklearn.linear_model import LinearRegression
from pyproj import Transformer

try:
    from tqdm import tqdm as _tqdm
except Exception:
    _tqdm = None


def tqdm_wrap(it: Iterable[Any], **kw: Any) -> Iterable[Any]:
    return _tqdm(it, **kw) if _tqdm else it


# ============================================================
# KONFIG
# ============================================================
ROTMAPPE = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Prediksjon\SDV"
OUT_DIR = ROTMAPPE

PRED_AR = 2026
MIN_AR = 2

# Tilstandsgrenser (1..5)
IRI_GRENSE = [1.5, 2.5, 4.0, 6.0]  # mm/m
SPOR_GRENSE = [10, 15, 20, 25]     # mm

# Transformer: EPSG:25833 (UTM33) -> EPSG:4326 (WGS84)
UTM33_TO_WGS84 = Transformer.from_crs(25833, 4326, always_xy=True)
# ============================================================


def iri_klasse(v: float | int | None) -> Optional[int]:
    if v is None or pd.isna(v):
        return None
    x = float(v)
    if x < IRI_GRENSE[0]:
        return 1
    if x < IRI_GRENSE[1]:
        return 2
    if x < IRI_GRENSE[2]:
        return 3
    if x < IRI_GRENSE[3]:
        return 4
    return 5


def spor_klasse(v: float | int | None) -> Optional[int]:
    if v is None or pd.isna(v):
        return None
    x = float(v)
    if x < SPOR_GRENSE[0]:
        return 1
    if x < SPOR_GRENSE[1]:
        return 2
    if x < SPOR_GRENSE[2]:
        return 3
    if x < SPOR_GRENSE[3]:
        return 4
    return 5


def safe_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", ".", regex=False), errors="coerce")


def normalize_headers(cols: List[str]) -> List[str]:
    return [re.sub(r"\s*\[.*?\]", "", c).strip() for c in cols]


def hent_veginfo_fra_filnavn(filnavn: str) -> Tuple[str, int]:
    """
    Støtter FV65, FV065, FV00065, FV00539, FV06000 osv.
    Normaliserer til 5 siffer: FV00065, FV00666, FV06000.
    """
    navn = filnavn.upper()

    m_felt = re.search(r"(?:^|[_\-])FELT\s*([0-9]+)(?:[_\-\.]|$)", navn)
    felt = int(m_felt.group(1)) if m_felt else 0

    m = re.search(r"_(FV|RV|EV|KV)0*(\d{1,5})_", navn)
    if not m:
        m = re.search(r"(?:^|[_\- ])(FV|RV|EV|KV)0*(\d{1,5})(?:[_\- ]|$)", navn)

    if not m:
        return "UKJENT", felt

    vegtype = m.group(1)
    vegnr = int(m.group(2))
    return f"{vegtype}{vegnr:05d}", felt


def hent_strekning_fra_filnavn(filnavn: str) -> str:
    """
    Tar første start-strekning fra filnavn:
      ..._S11D1_m00057-S12D1_m01215_... -> S11D1
    """
    navn = filnavn.upper()
    m = re.search(r"_(S\d{1,3}D\d)_", navn)
    if m:
        return m.group(1)
    m2 = re.search(r"(S\d{1,3}D\d)", navn)
    return m2.group(1) if m2 else "S0D0"


def les_sdv(filsti: str) -> Tuple[Optional[Dict[str, str]], Optional[pd.DataFrame]]:
    """
    Leser én .sdv, normaliserer kolonner og lager:
      - iri_mm_m (fra Alfred IRI / IRI / Class1 IRI)
      - spor_mm  (fra Spordybde)
    """
    try:
        raw = Path(filsti).read_bytes()
        if sum(1 for b in raw[:100] if b != 0) < 5:
            return None, None

        tekst = raw.decode("latin-1", errors="ignore")
        linjer = tekst.splitlines()

        header_idx = next((i for i, l in enumerate(linjer) if l.startswith("Utkjørt meter")), None)
        if header_idx is None:
            return None, None

        # Meta (ikke strengt nødvendig nå, men kan utvides senere)
        meta: Dict[str, str] = {}
        for linje in linjer[:header_idx]:
            if ";" in linje:
                k, v = linje.split(";", 1)
                meta[k.strip()] = v.strip()

        raw_cols = [c for c in linjer[header_idx].split(";") if c.strip()]
        cols = normalize_headers(raw_cols)

        rader: List[Dict[str, str]] = []
        for linje in linjer[header_idx + 1 :]:
            if not linje.strip():
                continue
            v = linje.rstrip(";").split(";")
            n = min(len(cols), len(v))
            if n >= 5:
                rader.append(dict(zip(cols[:n], v[:n])))

        if not rader:
            return None, None

        df = pd.DataFrame(rader)

        num_cols = [
            "Utkjørt meter", "Fra vegmeter", "Til vegmeter",
            "Spordybde", "Sporbredde", "Tverrfall",
            "Alfred IRI", "IRI", "Class1 IRI",
            "Breddegrad", "Lengdegrad",
            "Sone 33V N", "Sone 33V Ø",
            "Fra reflinkpos", "Til reflinkpos",
        ]
        for c in num_cols:
            if c in df.columns:
                df[c] = safe_float(df[c])

        # Normaliser IRI
        iri_series = None
        for c in ["Alfred IRI", "IRI", "Class1 IRI"]:
            if c not in df.columns:
                continue
            iri_series = df[c] if iri_series is None else iri_series.fillna(df[c])
        df["iri_mm_m"] = iri_series if iri_series is not None else np.nan

        # Normaliser spor
        df["spor_mm"] = df["Spordybde"] if "Spordybde" in df.columns else np.nan

        # Opptaksdato (valgfritt; beholdes hvis du vil bruke det senere)
        dato_str = meta.get("Opptaksdato", "")
        df["opptaksdato"] = dato_str[:10] if dato_str else ""

        return meta, df

    except Exception as e:
        print(f"  FEIL: {filsti}: {e}")
        return None, None


def ensure_latlon(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sikrer 'lat' og 'lon'.
    - Bruker Breddegrad/Lengdegrad hvis de ser ut som grader
    - Ellers transformer UTM33 (Sone 33V Ø/N)
    """
    df = df.copy()

    lat = df["Breddegrad"] if "Breddegrad" in df.columns else pd.Series(np.nan, index=df.index)
    lon = df["Lengdegrad"] if "Lengdegrad" in df.columns else pd.Series(np.nan, index=df.index)

    lat_ok = lat.between(50, 75)
    lon_ok = lon.between(0, 35)
    use_ll = lat_ok & lon_ok

    df["lat"] = np.where(use_ll, lat, np.nan)
    df["lon"] = np.where(use_ll, lon, np.nan)

    if "Sone 33V Ø" in df.columns and "Sone 33V N" in df.columns:
        need = df["lat"].isna() | df["lon"].isna()
        e = df.loc[need, "Sone 33V Ø"].astype(float)
        n = df.loc[need, "Sone 33V N"].astype(float)
        ok = pd.notna(e) & pd.notna(n)
        if ok.any():
            lon2, lat2 = UTM33_TO_WGS84.transform(e[ok].to_numpy(), n[ok].to_numpy())
            idx = e[ok].index
            df.loc[idx, "lat"] = lat2
            df.loc[idx, "lon"] = lon2

    return df


def les_alle_filer(rotmappe: str) -> pd.DataFrame:
    alle: List[pd.DataFrame] = []
    ar_mapper = sorted([p for p in Path(rotmappe).iterdir() if p.is_dir() and re.match(r"20\d{2}", p.name)])

    print(f"Fant {len(ar_mapper)} årmapper under {rotmappe}")
    for ar_mappe in ar_mapper:
        ar = int(ar_mappe.name)
        filer = sorted(ar_mappe.glob("*.sdv"))
        print(f"  {ar}: {len(filer)} .sdv-filer")

        for fil in tqdm_wrap(filer, desc=f"Leser {ar}", leave=False):
            _, df = les_sdv(str(fil))
            if df is None or df.empty:
                continue

            veg, felt = hent_veginfo_fra_filnavn(fil.name)
            strekning = hent_strekning_fra_filnavn(fil.name)

            df = ensure_latlon(df)

            df["ar"] = ar
            df["veg"] = veg
            df["felt"] = felt
            df["strekning"] = strekning

            # vm-grunnlag: bruk Utkjørt meter hvis finnes, ellers Fra vegmeter
            if "Utkjørt meter" in df.columns and df["Utkjørt meter"].notna().any():
                vm = pd.to_numeric(df["Utkjørt meter"], errors="coerce")
            elif "Fra vegmeter" in df.columns:
                vm = pd.to_numeric(df["Fra vegmeter"], errors="coerce")
            else:
                vm = pd.Series(np.nan, index=df.index)

            df["vm_bin"] = (np.floor(vm.to_numpy() / 20.0) * 20.0).astype(float)

            keep = [
                "ar", "veg", "felt", "strekning", "vm_bin",
                "iri_mm_m", "spor_mm",
                "lat", "lon",
                "opptaksdato",
                "Fra vegmeter", "Til vegmeter",
                "Fra strekning", "Fra reflinkid", "Fra reflinkpos",
            ]
            keep = [c for c in keep if c in df.columns]
            alle.append(df[keep])

    if not alle:
        raise ValueError(f"Ingen gyldige .sdv-filer funnet under {rotmappe}")

    master = pd.concat(alle, ignore_index=True)
    master = master.dropna(subset=["vm_bin"]).copy()

    print(f"\nMaster: {len(master):,} rader")
    return master


def make_segment_id(veg: str, strekning: str, felt: int, vm_bin: float) -> str:
    return f"{veg}_{strekning}_F{felt:02d}_M{int(round(vm_bin)):07d}"


def siste_gyldige(arr: np.ndarray) -> float:
    for v in arr[::-1]:
        if not np.isnan(v):
            return float(v)
    return np.nan


def regresjon_pred(grp: pd.DataFrame, col: str, pred_ar: int, min_ar: int) -> Tuple[float, float, float]:
    """
    Returnerer (pred, slope, r2).
    Filtrerer bort NaN før regresjon (sklearn tåler ikke NaN).
    """
    if col not in grp.columns:
        return np.nan, np.nan, np.nan

    y = grp[col].astype(float).to_numpy()
    x = grp["ar"].astype(int).to_numpy().reshape(-1, 1)

    ok = ~np.isnan(y)
    if int(ok.sum()) >= int(min_ar):
        model = LinearRegression().fit(x[ok], y[ok])
        pred = float(model.predict(np.array([[pred_ar]], dtype=int))[0])
        slope = float(model.coef_[0])
        r2 = float(model.score(x[ok], y[ok]))
        return max(0.0, pred), slope, r2

    return float(siste_gyldige(y[ok])), np.nan, np.nan


def bygg_outputs(master: pd.DataFrame, pred_ar: int, min_ar: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lager:
      - segmenter_df: 1 rad per (veg, felt, strekning, vm_bin)
      - serie_df: målt + predikert pr segment for dashboard
    """
    key_year = ["veg", "felt", "strekning", "vm_bin", "ar"]
    cols = [
        "iri_mm_m", "spor_mm",
        "lat", "lon",
        "opptaksdato",
        "Fra vegmeter", "Til vegmeter",
        "Fra strekning", "Fra reflinkid", "Fra reflinkpos",
    ]
    cols = [c for c in cols if c in master.columns]

    base = master[key_year + cols].copy().sort_values(key_year)
    # en rad per segment-år (siste forekomst)
    base_1 = base.drop_duplicates(subset=key_year, keep="last").copy()

    # Tidsserie målt
    serie_malt = pd.DataFrame(
        {
            "veg": base_1["veg"].astype(str),
            "felt": base_1["felt"].astype(int),
            "strekning": base_1["strekning"].astype(str),
            "vm_bin": base_1["vm_bin"].astype(float),
            "aar": base_1["ar"].astype(int),
            "datatype": "malt",
            "iri_mm_m": base_1["iri_mm_m"].astype(float) if "iri_mm_m" in base_1.columns else np.nan,
            "spor_mm": base_1["spor_mm"].astype(float) if "spor_mm" in base_1.columns else np.nan,
            "opptaksdato": base_1["opptaksdato"].astype(str) if "opptaksdato" in base_1.columns else "",
        }
    )

    serie_malt["segment_id"] = (
        serie_malt["veg"]
        + "_"
        + serie_malt["strekning"]
        + "_F"
        + serie_malt["felt"].astype(str).str.zfill(2)
        + "_M"
        + serie_malt["vm_bin"].round().astype(int).astype(str).str.zfill(7)
    )

    groups = base_1.groupby(["veg", "felt", "strekning", "vm_bin"], sort=False)
    n = len(groups)
    print(f"\nSegmenter: {n:,} (20m per strekning)")
    print("Regner regresjon og bygger segmenter (logger hver 20000)...")

    seg_rows: List[Dict[str, Any]] = []
    pred_rows: List[Dict[str, Any]] = []

    for i, ((veg_any, felt_any, strek_any, vm_any), grp) in enumerate(groups, start=1):
        if i % 20000 == 0:
            print(f"  ... {i:,}/{n:,}")

        veg = str(veg_any)
        felt = int(felt_any)
        strekning = str(strek_any)
        vm = float(vm_any)
        sid = make_segment_id(veg, strekning, felt, vm)

        grp = grp.sort_values("ar")

        iri_pred, iri_slope, iri_r2 = regresjon_pred(grp, "iri_mm_m", pred_ar, min_ar)
        spor_pred, spor_slope, spor_r2 = regresjon_pred(grp, "spor_mm", pred_ar, min_ar)

        siste_ar = int(grp["ar"].max())
        g_last = grp.iloc[-1]

        # siste gyldige måleverdi per parameter
        iri_siste = float(siste_gyldige(grp["iri_mm_m"].astype(float).to_numpy())) if "iri_mm_m" in grp.columns else np.nan
        spor_siste = float(siste_gyldige(grp["spor_mm"].astype(float).to_numpy())) if "spor_mm" in grp.columns else np.nan

        # Geometri: siste gyldige lat/lon
        grp_xy = grp.dropna(subset=["lat", "lon"])
        g_pos = grp_xy.iloc[-1] if len(grp_xy) else g_last

        seg_rows.append(
            {
                "segment_id": sid,
                "veg": veg,
                "felt": felt,
                "strekning": strekning,
                "vm_bin": vm,
                "siste_ar": siste_ar,
                "opptaksdato_siste": str(g_pos.get("opptaksdato", "")),
                "lat": float(g_pos.get("lat", np.nan)),
                "lon": float(g_pos.get("lon", np.nan)),
                "iri_siste": iri_siste,
                "spor_siste": spor_siste,
                "iri_pred": float(iri_pred) if pd.notna(iri_pred) else np.nan,
                "spor_pred": float(spor_pred) if pd.notna(spor_pred) else np.nan,
                "klasse_iri_siste": iri_klasse(iri_siste),
                "klasse_spor_siste": spor_klasse(spor_siste),
                "klasse_iri_pred": iri_klasse(iri_pred),
                "klasse_spor_pred": spor_klasse(spor_pred),
                "iri_slope": float(iri_slope) if pd.notna(iri_slope) else np.nan,
                "spor_slope": float(spor_slope) if pd.notna(spor_slope) else np.nan,
                "iri_r2": float(iri_r2) if pd.notna(iri_r2) else np.nan,
                "spor_r2": float(spor_r2) if pd.notna(spor_r2) else np.nan,
            }
        )

        pred_rows.append(
            {
                "segment_id": sid,
                "veg": veg,
                "felt": felt,
                "strekning": strekning,
                "vm_bin": vm,
                "aar": int(pred_ar),
                "datatype": "predikert",
                "iri_mm_m": float(iri_pred) if pd.notna(iri_pred) else np.nan,
                "spor_mm": float(spor_pred) if pd.notna(spor_pred) else np.nan,
                "opptaksdato": "",
            }
        )

    segmenter_df = pd.DataFrame(seg_rows)
    serie_pred = pd.DataFrame(pred_rows)
    serie_df = pd.concat([serie_malt, serie_pred], ignore_index=True)

    return segmenter_df, serie_df


def skriv_segmenter_gpkg(segmenter_df: pd.DataFrame, gpkg_path: str) -> None:
    geom = [
        Point(lon, lat) if pd.notna(lat) and pd.notna(lon) else None
        for lat, lon in zip(segmenter_df["lat"].to_numpy(), segmenter_df["lon"].to_numpy())
    ]
    gdf = gpd.GeoDataFrame(segmenter_df, geometry=geom, crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notna()].reset_index(drop=True)
    gdf.to_file(gpkg_path, driver="GPKG", layer="segmenter")
    print(f"✅ Skrev GPKG segmenter: {gpkg_path} (n={len(gdf):,})")


def main() -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    gpkg_path = str(Path(OUT_DIR) / f"segmenter_{ts}.gpkg")
    csv_path = str(Path(OUT_DIR) / f"tilstand_serie_{ts}.csv")

    master = les_alle_filer(ROTMAPPE)

    # (valgfritt) små sanity prints
    print("Rader master:", len(master))
    print("NaN vm_bin:", int(master["vm_bin"].isna().sum()))
    print("\nFelt-fordeling:")
    print(master["felt"].value_counts(dropna=False).head(10))
    print("\nTopp 20 veg-koder:")
    print(master["veg"].value_counts().head(20))
    print("\nUKJENT rader:", int((master["veg"] == "UKJENT").sum()))

    segmenter_df, serie_df = bygg_outputs(master, PRED_AR, MIN_AR)

    skriv_segmenter_gpkg(segmenter_df, gpkg_path)
    serie_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"✅ Skrev CSV tilstand_serie: {csv_path} (n={len(serie_df):,})")

    print("\nArcGIS-oppsett:")
    print("  1) Legg til gpkg: layer 'segmenter'")
    print("  2) Legg til CSV 'tilstand_serie' som tabell")
    print("  3) Bruk segment_id for filtering/join i Dashboard/Experience Builder")
    print("     Linechart spor: X=aar, Y=spor_mm, split=datatype")
    print("     Linechart iri:  X=aar, Y=iri_mm_m, split=datatype")


if __name__ == "__main__":
    main()
