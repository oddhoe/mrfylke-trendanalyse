# nvdb_to_gdb.py
#
# 01 – NVDB → GDB (endelig versjon)
#
# Bygger:
# - Vegnett
# - Bruer (tillat vekt)
# - Bruksklasse tømmer (900):
#     * BK_VERDI     (tonn)
#     * MAKS_LENGDE  (meter, inkl. spesielle begrensninger)
#
# Konsistent med:
# - 02_bygg_tillat_profil.py
# - 03_korridor_dim_kilde.py

import os
import re
import requests
import arcpy

# -------------------------
# KONFIG
# -------------------------
FYLKE = 15
SRID = 5973

NVDB_API = "https://nvdbapiles.atlas.vegvesen.no"
VEGNETT_API = f"{NVDB_API}/vegnett/api/v4"
VEGOBJ_API = f"{NVDB_API}/vegobjekter/api/v4"

OUT_GDB = r"D:\Conda\Flaskehalser\gdb\nvdb_radata.gdb"

HEADERS = {
    "X-Client": "nvdb_script",
    "Accept": "application/vnd.vegvesen.nvdb-v3-rev1+json"
}

# TLS / CA-bundle
CA_BUNDLE = os.environ.get("REQUESTS_CA_BUNDLE")  # Absolutt sti til PEM/bundle
ALLOW_INSECURE_FALLBACK = True  # Sett False for å feile hardt hvis CA er ugyldig

arcpy.env.overwriteOutput = True


# -------------------------
# HJELP
# -------------------------
def log(msg):
    print(msg)


def iter_paged(url, params):
    """
    Generator for NVDB-sider med robust TLS-håndtering.
    - Bruker REQUESTS_CA_BUNDLE hvis gyldig fil
    - Faller tilbake til verify=False (midlertidig) hvis tillatt
    """
    offset = None

    # Bestem verify-argument
    verify_arg = True  # system / requests default
    if CA_BUNDLE:
        if os.path.isfile(CA_BUNDLE):
            verify_arg = CA_BUNDLE
            log(f"[TLS] Bruker CA-bundle: {CA_BUNDLE}")
        else:
            msg = (
                f"[TLS] REQUESTS_CA_BUNDLE satt, men filen finnes ikke: {CA_BUNDLE}. "
                f"{'Slår av verifisering midlertidig.' if ALLOW_INSECURE_FALLBACK else 'Avslutter.'}"
            )
            log(msg)
            if ALLOW_INSECURE_FALLBACK:
                verify_arg = False
            else:
                raise OSError(f"Ugyldig REQUESTS_CA_BUNDLE: {CA_BUNDLE}")

    while True:
        p = dict(params)
        if offset:
            p["start"] = offset

        r = requests.get(
            url,
            params=p,
            headers=HEADERS,
            verify=verify_arg,
            timeout=30
        )
        r.raise_for_status()

        data = r.json()
        objs = data.get("objekter", [])
        if not objs:
            break

        for o in objs:
            yield o

        meta = data.get("metadata", {})
        nxt = meta.get("neste")
        if not nxt:
            break
        offset = nxt.get("start")


def to_geometry(geom):
    if not geom or "wkt" not in geom:
        return None
    try:
        return arcpy.FromWKT(geom["wkt"], arcpy.SpatialReference(SRID))
    except Exception:
        return None


def create_gdb(path):
    folder, name = os.path.split(path)
    if not os.path.exists(folder):
        os.makedirs(folder)
    if not arcpy.Exists(path):
        log(f"Oppretter GDB: {path}")
        arcpy.management.CreateFileGDB(folder, name)


def create_fc(gdb, name, geom_type, extra_fields):
    fc = os.path.join(gdb, name)
    if arcpy.Exists(fc):
        arcpy.management.Delete(fc)

    arcpy.management.CreateFeatureclass(
        gdb,
        name,
        geom_type,
        spatial_reference=SRID
    )

    arcpy.management.AddField(fc, "VEGLENKESEKV_ID", "LONG")
    arcpy.management.AddField(fc, "STARTPOS", "DOUBLE")
    arcpy.management.AddField(fc, "SLUTTPOS", "DOUBLE")

    for f in extra_fields:
        if len(f) == 2:
            arcpy.management.AddField(fc, f[0], f[1])
        else:
            arcpy.management.AddField(fc, f[0], f[1], field_length=f[2])

    return fc


# -------------------------
# 1) VEGNETT
# -------------------------
def hent_vegnett():
    log("Henter vegnett…")

    fc = create_fc(
        OUT_GDB,
        "Vegnett",
        "POLYLINE",
        [("VEGKATEGORI", "TEXT", 1), ("VEGNUMMER", "LONG")]
    )

    url = f"{VEGNETT_API}/veglenkesekvenser/segmentert"
    params = {
        "fylke": FYLKE,
        "vegsystemreferanse": "F",
        "antall": 5000,
        "inkluderAntall": "false"
    }

    cnt = 0
    with arcpy.da.InsertCursor(
        fc,
        ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "VEGKATEGORI", "VEGNUMMER"]
    ) as cur:

        for seg in iter_paged(url, params):
            vr = seg.get("vegsystemreferanse", {})
            if vr.get("strekning", {}).get("trafikantgruppe") != "K":
                continue

            geom = to_geometry(seg.get("geometri"))
            if not geom:
                continue

            cur.insertRow((
                geom,
                int(seg["veglenkesekvensid"]),
                float(seg.get("startposisjon", 0)),
                float(seg.get("sluttposisjon", 0)),
                vr.get("vegsystem", {}).get("vegkategori"),
                vr.get("vegsystem", {}).get("nummer")
            ))
            cnt += 1

    log(f"Vegnett ferdig: {cnt}")


# -------------------------
# 2) BRUER (vekt)
# -------------------------
def hent_bruer():
    log("Henter bruer…")

    fc = create_fc(
        OUT_GDB,
        "Bruer",
        "POLYLINE",
        [("TILLATT_TONN", "DOUBLE")]
    )

    url = f"{VEGOBJ_API}/vegobjekter/60"
    params = {
        "fylke": FYLKE,
        "vegsystemreferanse": "F",
        "antall": 1000,
        "inkluder": "egenskaper,lokasjon,geometri"
    }

    cnt = 0
    with arcpy.da.InsertCursor(
        fc,
        ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS", "TILLATT_TONN"]
    ) as cur:

        for o in iter_paged(url, params):
            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue
            if geom.type == "polygon":
                geom = geom.boundary()

            tillatt = None
            for e in o.get("egenskaper", []):
                if e["id"] == 12653:  # Brukslast
                    m = re.search(r"/(\d+)", str(e.get("verdi", "")))
                    if m:
                        tillatt = float(m.group(1))

            if tillatt is None:
                continue

            for s in o.get("lokasjon", {}).get("stedfestinger", []):
                if s.get("veglenkesekvensid"):
                    cur.insertRow((
                        geom,
                        int(s["veglenkesekvensid"]),
                        float(s.get("startposisjon", 0)),
                        float(s.get("sluttposisjon", 0)),
                        tillatt
                    ))
                    cnt += 1

    log(f"Bruer ferdig: {cnt}")


# -------------------------
# 3) BRUKSKLASSE TØMMER (900)
# -------------------------
def hent_bruksklasse():
    log("Henter bruksklasse tømmer (900)…")

    fc = create_fc(
        OUT_GDB,
        "Bruksklasse",
        "POLYLINE",
        [
            ("BK_VERDI", "LONG"),
            ("BK_TEKST", "TEXT", 50),
            ("MAKS_LENGDE", "DOUBLE"),
        ]
    )

    url = f"{VEGOBJ_API}/vegobjekter/900"
    params = {
        "fylke": FYLKE,
        "vegsystemreferanse": "F",
        "antall": 1000,
        "inkluder": "egenskaper,lokasjon,geometri"
    }

    cnt = 0
    with arcpy.da.InsertCursor(
        fc,
        ["SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS",
         "BK_VERDI", "BK_TEKST", "MAKS_LENGDE"]
    ) as cur:

        for o in iter_paged(url, params):
            bk_verdi = None
            bk_tekst = None
            maks_len = None

            for e in o.get("egenskaper", []):
                eid = e["id"]
                val = str(e.get("verdi", "")).strip()

                # BK / tonn
                if eid == 10897:
                    bk_tekst = val
                    tall = [int(t) for t in re.findall(r"\d+", val)]
                    if tall:
                        bk_verdi = max(tall)

                # Maks vogntoglengde (ENUM)
                elif eid == 10909:
                    try:
                        maks_len = float(val.replace(",", "."))
                    except Exception:
                        pass

                # Spesiell begrensning (f.eks. Trollstigen 13,3 m)
                elif "spesiell" in e.get("navn", "").lower():
                    m = re.search(r"(\d+[.,]\d+|\d+)\s*m", val)
                    if m:
                        spes = float(m.group(1).replace(",", "."))
                        if maks_len is None or spes < maks_len:
                            maks_len = spes

            if not bk_verdi:
                continue

            geom = to_geometry(o.get("geometri"))
            if not geom:
                continue

            for s in o.get("lokasjon", {}).get("stedfestinger", []):
                if s.get("veglenkesekvensid"):
                    cur.insertRow((
                        geom,
                        int(s["veglenkesekvensid"]),
                        float(s.get("startposisjon", 0)),
                        float(s.get("sluttposisjon", 0)),
                        bk_verdi,
                        bk_tekst,
                        maks_len
                    ))
                    cnt += 1

    log(f"Bruksklasse ferdig: {cnt}")


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    create_gdb(OUT_GDB)
    hent_vegnett()
    hent_bruer()
    hent_bruksklasse()
    log("✅ NVDB → GDB ferdig (01)")