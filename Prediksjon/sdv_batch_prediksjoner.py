#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sdv_batch_prediksjoner.py
=========================
Leser ALLE Viatech ViaPPS .sdv-filer fra mappestruktur med årganger,
bygger temporal regresjonsmodell per 20m-segment og predikerer neste år.

Mappestruktur (sett ROTMAPPE):
    C:\SDV\
        2021\  ← inneholder *.sdv-filer
        2022\
        2023\
        2024\
        2025\

Output:
    tilstand_alle_veger_YYYYMMDD.gpkg  ← klar for ArcGIS Pro
    tilstand_alle_veger_YYYYMMDD.xlsx

Avhengigheter:
    pip install geopandas pandas openpyxl scikit-learn shapely tqdm
"""

import os, re, glob
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from datetime import datetime
from shapely.geometry import LineString, Point
from sklearn.linear_model import LinearRegression

try:
    from tqdm import tqdm
    TQDM = True
except ImportError:
    TQDM = False
    def tqdm(x, **kw): return x

# ============================================================
# KONFIGURASJON
# ============================================================
ROTMAPPE    = r"C:\SDV"                 # Rotmappen med år-mapper
PRED_AR     = 2026                      # Prediksjonsår
MIN_AR      = 2                         # Minimum år med data for å inkludere segment
GPKG_UT     = r"C:\SDV\tilstand_alle_veger.gpkg"
XLSX_UT     = r"C:\SDV\tilstand_alle_veger.xlsx"
# ============================================================


def les_sdv(filsti):
    """Parser én .sdv-fil. Returnerer (meta_dict, DataFrame)."""
    try:
        with open(filsti, "rb") as f:
            raw = f.read()
        # Sjekk at filen ikke er tom/korrupt
        if len([b for b in raw[:100] if b != 0]) < 5:
            return None, None
        tekst = raw.decode("latin-1")
        linjer = tekst.splitlines()

        header_idx = None
        for i, linje in enumerate(linjer):
            if linje.startswith("Utkjørt meter"):
                header_idx = i
                break
        if header_idx is None:
            return None, None

        meta = {}
        for linje in linjer[:header_idx]:
            if ";" in linje:
                d = linje.split(";", 1)
                meta[d[0].strip()] = d[1].strip()

        def rens(k): return re.sub(r'\s*\[.*?\]', '', k).strip()
        kolonner = [rens(k) for k in linjer[header_idx].split(";") if k.strip()]

        rader = []
        for linje in linjer[header_idx + 1:]:
            if not linje.strip(): continue
            v = linje.rstrip(";").split(";")
            if len(v) >= 10:
                rader.append(dict(zip(kolonner, v[:len(kolonner)])))

        if not rader:
            return None, None

        df = pd.DataFrame(rader)
        for kol in ["Utkjørt meter","Fra vegmeter","Til vegmeter","Fra reflinkpos","Til reflinkpos",
                    "Spordybde","Sporbredde","Tverrfall","Kurveradius","Alfred IRI","MPD",
                    "Hastighet","Høyde","Breddegrad","Lengdegrad","Sone 33V N","Sone 33V Ø"]:
            if kol in df.columns:
                df[kol] = pd.to_numeric(df[kol].str.replace(",", ".", regex=False), errors="coerce")

        # Trekk ut opptaksdato og vegnavn fra metadata
        dato_str = meta.get("Opptaksdato", "")
        df["opptaksdato"] = dato_str[:10] if dato_str else ""

        return meta, df
    except Exception as e:
        print(f"  FEIL: {filsti}: {e}")
        return None, None


def hent_veginfo_fra_filnavn(filnavn):
    """
    Parser filnavnmønsteret:
      NVDB_Fy15_FV060_S1D1_m00005-S1D1_m07782_felt1___20240430-112842.sdv
    Returnerer (veg, felt) – f.eks. ("FV060", 1)
    """
    m = re.search(r'_((?:FV|RV|EV|KV)\d+)_', filnavn, re.IGNORECASE)
    veg = m.group(1).upper() if m else "UKJENT"
    m2 = re.search(r'_felt(\d+)', filnavn, re.IGNORECASE)
    felt = int(m2.group(1)) if m2 else 0
    return veg, felt


def les_alle_filer(rotmappe):
    """
    Skanner rotmappe/YYYY/*.sdv og returnerer én stor DataFrame
    med kolonnene: ar, veg, felt, vm_rund, + måleparametere + geometri.
    """
    alle = []
    ar_mapper = sorted([
        p for p in Path(rotmappe).iterdir()
        if p.is_dir() and re.match(r'20\d{2}', p.name)
    ])

    print(f"Fant {len(ar_mapper)} årmapper under {rotmappe}")
    for ar_mappe in ar_mapper:
        ar = int(ar_mappe.name)
        filer = sorted(ar_mappe.glob("*.sdv"))
        print(f"  {ar}: {len(filer)} .sdv-filer")

        for fil in tqdm(filer, desc=f"  Leser {ar}", leave=False):
            meta, df = les_sdv(str(fil))
            if df is None or len(df) == 0:
                continue
            veg, felt = hent_veginfo_fra_filnavn(fil.name)
            df["ar"]       = ar
            df["veg"]      = veg
            df["felt"]     = felt
            df["vm_rund"]  = (df["Fra vegmeter"] / 20).round() * 20
            alle.append(df[[
                "ar","veg","felt","vm_rund",
                "Fra vegmeter","Til vegmeter","Fra strekning",
                "Fra reflinkid","Fra reflinkpos",
                "Breddegrad","Lengdegrad","Sone 33V N","Sone 33V Ø",
                "Spordybde","Sporbredde","Tverrfall","Alfred IRI","MPD",
                "Hastighet","opptaksdato"
            ]])

    if not alle:
        raise ValueError(f"Ingen gyldige .sdv-filer funnet under {rotmappe}")

    master = pd.concat(alle, ignore_index=True)
    print(f"\nMaster-tabell: {len(master):,} rader totalt")
    return master


def prediker_alle_segmenter(master, pred_ar):
    """
    Kjørn temporal regresjon per (veg, felt, vm_rund).
    Returnerer én rad per unikt segment med alle år + prediksjon.
    """
    years_i_data = sorted(master["ar"].unique())
    resultater   = []

    grupper = master.groupby(["veg", "felt", "vm_rund"])
    print(f"\nRegresjon over {len(grupper)} unike 20m-segmenter...")

    for (veg, felt, vm), grp in tqdm(grupper, desc="Regresjon"):
        grp = grp.sort_values("ar")
        siste = grp.iloc[-1]
        rad = {
            "veg": veg, "felt": felt, "vm_rund": vm,
            "n_ar": len(grp),
            "ar_liste": ",".join(str(a) for a in grp["ar"].values),
            "Fra vegmeter": siste["Fra vegmeter"],
            "Til vegmeter": siste["Til vegmeter"],
            "Fra strekning": siste.get("Fra strekning",""),
            "Fra reflinkid": siste.get("Fra reflinkid",""),
            "Fra reflinkpos": siste.get("Fra reflinkpos",""),
            "Breddegrad": siste["Breddegrad"],
            "Lengdegrad": siste["Lengdegrad"],
            "Sone 33V N": siste.get("Sone 33V N",""),
            "Sone 33V Ø": siste.get("Sone 33V Ø",""),
            "opptaksdato_siste": siste["opptaksdato"],
        }

        # Verdier per år
        for ar in years_i_data:
            rader_ar = grp[grp["ar"] == ar]
            if len(rader_ar):
                rad[f"spor_{ar}"] = rader_ar.iloc[0]["Spordybde"]
                rad[f"iri_{ar}"]  = rader_ar.iloc[0]["Alfred IRI"]
            else:
                rad[f"spor_{ar}"] = np.nan
                rad[f"iri_{ar}"]  = np.nan

        # Temporal regresjon
        for kol, prefix in [("Spordybde","spor"), ("Alfred IRI","iri")]:
            y_vals = grp[kol].values.astype(float)
            x_vals = grp["ar"].values.reshape(-1,1)
            gyldige = ~np.isnan(y_vals)
            if gyldige.sum() >= MIN_AR:
                model = LinearRegression().fit(x_vals[gyldige], y_vals[gyldige])
                pred  = float(model.predict([[pred_ar]])[0])
                rad[f"{prefix}_pred_{pred_ar}"] = round(max(0.0, pred), 2)
                rad[f"{prefix}_slope"]          = round(float(model.coef_[0]), 4)
                rad[f"{prefix}_r2"]             = round(float(model.score(
                                                      x_vals[gyldige], y_vals[gyldige])), 3)
            else:
                # Ikke nok data – bruk siste verdi
                rad[f"{prefix}_pred_{pred_ar}"] = y_vals[-1] if gyldige.any() else np.nan
                rad[f"{prefix}_slope"]          = np.nan
                rad[f"{prefix}_r2"]             = np.nan

        resultater.append(rad)

    return pd.DataFrame(resultater)


def legg_til_klasser(df, pred_ar):
    def iri_k(v):
        if pd.isna(v): return None
        return 1 if v<1.5 else 2 if v<2.5 else 3 if v<4.0 else 4 if v<6.0 else 5
    def spor_k(v):
        if pd.isna(v): return None
        return 1 if v<10 else 2 if v<15 else 3 if v<20 else 4 if v<25 else 5

    siste_ar = df["ar_liste"].str.split(",").apply(lambda x: int(x[-1])).max() if len(df) else pred_ar-1
    # Klasse for siste faktiske år og prediksjonsår
    for ar in [siste_ar, pred_ar]:
        if f"iri_{ar}" in df.columns:
            df[f"klasse_iri_{ar}"]  = df[f"iri_{ar}"].apply(iri_k)
            df[f"klasse_spor_{ar}"] = df[f"spor_{ar}"].apply(spor_k)
    return df


def bygg_linjegeometri(df):
    lats, lons = df["Breddegrad"].values, df["Lengdegrad"].values
    geom = []
    for i in range(len(df)):
        if i < len(df)-1 and all(pd.notna([lats[i],lons[i],lats[i+1],lons[i+1]])):
            geom.append(LineString([(lons[i],lats[i]),(lons[i+1],lats[i+1])]))
        elif pd.notna(lats[i]) and pd.notna(lons[i]):
            geom.append(Point(lons[i], lats[i]))
        else:
            geom.append(None)
    return geom


def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    # 1. Les alle filer
    master = les_alle_filer(ROTMAPPE)

    # 2. Regresjon og prediksjon
    res = prediker_alle_segmenter(master, PRED_AR)

    # 3. Tilstandsklasser
    res = legg_til_klasser(res, PRED_AR)

    # 4. Geometri
    geom = bygg_linjegeometri(res)

    # 5. GeoPackage
    gdf = gpd.GeoDataFrame(res, geometry=geom, crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notna()].reset_index(drop=True)
    gpkg_ut = GPKG_UT.replace(".gpkg", f"_{ts}.gpkg")
    gdf.to_file(gpkg_ut, driver="GPKG", layer="tilstand_prediksjoner")
    print(f"\n✅ GeoPackage: {gpkg_ut}  ({len(gdf):,} segmenter)")

    # 6. Excel (uten geometri)
    xlsx_ut = XLSX_UT.replace(".xlsx", f"_{ts}.xlsx")
    res.drop(columns=[c for c in res.columns if c == "geometry"], errors="ignore")\
       .to_excel(xlsx_ut, index=False, sheet_name="tilstand")
    print(f"✅ Excel:       {xlsx_ut}")

    print(f"\nFerdig! Åpne .gpkg i ArcGIS Pro og symboliser på:")
    print(f"  - klasse_iri_{PRED_AR}   (IRI-klasse predikert)")
    print(f"  - klasse_spor_{PRED_AR}  (Spordybdeklasse predikert)")
    print(f"  - iri_slope              (forverringshastighet IRI mm/m per år)")
    print(f"  - spor_slope             (forverringshastighet spordybde mm per år)")


if __name__ == "__main__":
    main()
