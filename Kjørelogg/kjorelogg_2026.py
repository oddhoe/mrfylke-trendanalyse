# -*- coding: utf-8 -*-
"""
Kjørelogg FV Møre og Romsdal – bygg vegnettlag (K og G) til GPKG

- Henter segmenterte veglenkesekvenser fra NVDB Les API v4 (vegnett)
- Filtrerer til vegkategori F (Fylkesveg) og fylke=15
- Splitter på trafikantgruppe K (kjørende) og G (gående/syklende)
- Legger på feltene du trenger (status, måledato, operatør, bil, kommentar, driftskontrakt)
- Skriver til GeoPackage med to lag: fv_k og fv_g

Kilde/endepunkt: https://nvdbapiles.atlas.vegvesen.no/vegnett/api/v4/veglenkesekvenser  (NVDB API Les v4, vegnett)  :contentReference[oaicite:1]{index=1}
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from typing import Dict, Any, Iterable, Optional, Tuple

import requests

# Prøv å bruke shapely+fiona for GPKG-skriving
try:
    from shapely import wkt as shapely_wkt
    import fiona
    from fiona.crs import from_epsg
except Exception as e:
    shapely_wkt = None
    fiona = None
    from_epsg = None


NVDB_BASE = "https://nvdbapiles.atlas.vegvesen.no"
VEGNETT_URL = f"{NVDB_BASE}/vegnett/api/v4/veglenkesekvenser"


STATUS_DOMAIN = ["Ikke målt", "Påbegynt", "Måles ikke", "Ferdig målt"]


def nvdb_get(url: str, params: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    headers = {
        "Accept": "application/json",
        # Greit å identifisere klienten ved feilsøking hos SVV, men ikke påkrevd
        "User-Agent": "mrfk-kjorelogg-fv/1.0",
    }
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def is_fylkesveg(obj: Dict[str, Any]) -> bool:
    # V4-respons har vegsystemreferanse.vegsystem.vegkategori (f.eks "F")
    try:
        return obj["vegsystemreferanse"]["vegsystem"]["vegkategori"] == "F"
    except Exception:
        return False


def trafikantgruppe(obj: Dict[str, Any]) -> Optional[str]:
    # Typisk ligger denne på vegsystemreferanse.strekning.trafikantgruppe ("K", "G", ...)
    try:
        return obj["vegsystemreferanse"]["strekning"]["trafikantgruppe"]
    except Exception:
        return None


def iter_veglenkesekvenser(
    fylke: int,
    antall: int = 1000,
    sortert: bool = False,
    inkluder_antall: bool = False,
    ekstra_params: Optional[Dict[str, Any]] = None,
    sleep_s: float = 0.0,
) -> Iterable[Dict[str, Any]]:
    """
    Generator som paginerer gjennom vegnett/api/v4/veglenkesekvenser.
    Paginering i v4: metadata.neste.start (og evt metadata.neste.href). :contentReference[oaicite:2]{index=2}
    """
    start = None
    params = {
        "fylke": fylke,
        "antall": antall,
        "sortert": str(sortert).lower(),
        "inkluderAntall": str(inkluder_antall).lower(),
        # merk: "start" settes etter første side
    }
    if ekstra_params:
        params.update(ekstra_params)

    page = 0
    while True:
        if start:
            params["start"] = start
        else:
            params.pop("start", None)

        page += 1
        data = nvdb_get(VEGNETT_URL, params=params)

        objekter = data.get("objekter") or []
        for o in objekter:
            yield o

        meta = data.get("metadata") or {}
        neste = meta.get("neste") or {}
        start = neste.get("start")

        if not start:
            break

        if sleep_s > 0:
            time.sleep(sleep_s)


def build_feature(
    obj: Dict[str, Any],
    default_status: str = "Ikke målt",
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Returnerer (properties, geometry_geojsonlike) for GPKG/GeoJSON-skriving.
    NVDB geometri leveres som WKT + srid (typisk 5973 for vegnett). :contentReference[oaicite:3]{index=3}
    """
    geom = obj.get("geometri") or {}
    wkt = geom.get("wkt")
    srid = geom.get("srid")

    if not wkt:
        return None

    tg = trafikantgruppe(obj)
    vsr = obj.get("vegsystemreferanse") or {}
    vegsys = (vsr.get("vegsystem") or {})
    strek = (vsr.get("strekning") or {})

    props = {
        # NVDB-identifikatorer
        "veglenkesekvensid": obj.get("veglenkesekvensid"),
        "veglenkenummer": obj.get("veglenkenummer"),
        "segmentnummer": obj.get("segmentnummer"),
        "referanse": obj.get("referanse"),
        "kortform": obj.get("kortform"),

        # Vegsystemreferanse (nyttig i rapportering/søk)
        "vegkategori": vegsys.get("vegkategori"),
        "fase": vegsys.get("fase"),
        "vegnummer": vegsys.get("nummer"),
        "strekning": strek.get("strekning"),
        "delstrekning": strek.get("delstrekning"),
        "trafikantgruppe": tg,
        "retning": strek.get("retning"),
        "fra_meter": strek.get("fra_meter"),
        "til_meter": strek.get("til_meter"),

        # Nøkkeltall
        "lengde_m": obj.get("lengde"),

        # Kjørelogg-felter (klare for domener/valglister i AGOL)
        "status_maling": default_status,
        "maledato": None,  # date
        "malebiloper": None,
        "malebil": None,
        "kommentar_mbo": None,
        "driftskontr": None,
    }

    # GeoJSON-lignende geometri for fiona: shapely -> mapping
    if shapely_wkt is None:
        raise RuntimeError("Mangler shapely. Installer: pip install shapely fiona")

    g = shapely_wkt.loads(wkt)
    geometry = {
        "type": g.geom_type,
        "coordinates": getattr(g, "__geo_interface__", {}).get("coordinates", None),
    }
    if geometry["coordinates"] is None:
        geometry = g.__geo_interface__

    # CRS håndteres i layer-oppsett; vi returnerer srid for sanity-check
    props["_srid"] = srid
    return props, geometry


def ensure_status(value: str) -> str:
    if value not in STATUS_DOMAIN:
        raise ValueError(f"Ugyldig status '{value}'. Må være en av: {STATUS_DOMAIN}")
    return value


def write_gpkg(
    out_path: str,
    layer_name: str,
    features: Iterable[Tuple[Dict[str, Any], Dict[str, Any]]],
    epsg: int = 5973,
) -> int:
    """
    Skriver features til GeoPackage-lag.
    """
    if fiona is None:
        raise RuntimeError("Mangler fiona. Installer: pip install fiona shapely")

    schema = {
        "geometry": "LineString",
        "properties": {
            "veglenkesekvensid": "int",
            "veglenkenummer": "int",
            "segmentnummer": "int",
            "referanse": "str",
            "kortform": "str",
            "vegkategori": "str",
            "fase": "str",
            "vegnummer": "int",
            "strekning": "int",
            "delstrekning": "int",
            "trafikantgruppe": "str",
            "retning": "str",
            "fra_meter": "float",
            "til_meter": "float",
            "lengde_m": "float",
            "status_maling": "str",
            "maledato": "date",
            "malebiloper": "str",
            "malebil": "str",
            "kommentar_mbo": "str",
            "driftskontr": "str",
            "_srid": "int",
        },
    }

    # NB: geometrytype kan variere (MultiLineString). Vi skriver som "Unknown" hvis nødvendig:
    # For enkelhet: åpne først med LineString og la fiona håndtere MultiLineString ved behov.
    count = 0
    with fiona.open(
        out_path,
        mode="w" if layer_name == "fv_k" else "a",
        driver="GPKG",
        layer=layer_name,
        schema=schema,
        crs=from_epsg(epsg),
    ) as dst:
        for props, geom in features:
            dst.write({"type": "Feature", "properties": props, "geometry": geom})
            count += 1
    return count


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fylke", type=int, default=15, help="Fylkesnummer (Møre og Romsdal=15)")
    ap.add_argument("--out", required=True, help="Output .gpkg fil, f.eks G:\\Test\\kjorelogg_fv_mr.gpkg")
    ap.add_argument("--epsg", type=int, default=5973, help="EPSG for vegnettgeometri (default 5973)")
    ap.add_argument("--antall", type=int, default=1000, help="Sidestørrelse mot API")
    ap.add_argument("--sleep", type=float, default=0.0, help="Pause (sek) mellom sider")
    ap.add_argument("--default-status", default="Ikke målt", help="Standard statusverdi")
    args = ap.parse_args()

    default_status = ensure_status(args.default_status)

    # Samle og splitte features på trafikantgruppe
    def gen_filtered(tg_wanted: str):
        for obj in iter_veglenkesekvenser(
            fylke=args.fylke,
            antall=args.antall,
            sortert=False,
            inkluder_antall=False,
            ekstra_params=None,
            sleep_s=args.sleep,
        ):
            if not is_fylkesveg(obj):
                continue

            tg = trafikantgruppe(obj)
            if tg != tg_wanted:
                continue

            built = build_feature(obj, default_status=default_status)
            if built is None:
                continue

            yield built

    print(f"[{dt.datetime.now().isoformat(timespec='seconds')}] Henter FV (fylke={args.fylke}) og skriver GPKG...")
    print("  - Layer fv_k: trafikantgruppe=K")
    n_k = write_gpkg(args.out, "fv_k", gen_filtered("K"), epsg=args.epsg)

    print("  - Layer fv_g: trafikantgruppe=G")
    n_g = write_gpkg(args.out, "fv_g", gen_filtered("G"), epsg=args.epsg)

    print(f"Ferdig. Skrev {n_k} features i fv_k og {n_g} features i fv_g.")
    print(f"Output: {args.out}")

    # Liten sanity: minner om domener i AGOL
    print("\nNeste steg i ArcGIS Online (kort):")
    print("1) Publiser .gpkg som Hosted Feature Layer (to lag).")
    print("2) Sett 'Status måling' som coded value domain:")
    print("   " + " | ".join(STATUS_DOMAIN))
    print("3) Symboliser på status_maling.")
    print("4) Driftskontrakt: bruk 'Select by Location' eller spatial join mot kontraktspolygoner.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
