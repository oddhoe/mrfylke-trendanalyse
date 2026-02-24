# backfill_bruer_nulls.py
"""
Leser ALLE_EG fra Bruer-FC og populerer felt som er <Null>.
Kjøres etter nvdb_to_gdb uten å laste ned NVDB på nytt.
"""

import arcpy
import os
import re

arcpy.env.overwriteOutput = True

GDB    = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"
FC     = os.path.join(GDB, "Bruer")

# ALLE_EG-nøkkel → GDB-felt + konverteringsfunksjon
# Nøkkel er lowercase, matcher substring i ALLE_EG-token
FELT_MAP = {
    "byggverkstype":     ("BRUTYPE",          str),
    "lengde":            ("LENGDE_M",         float),
    "status":            ("TRAFIKKSTATUS",     str),
    "driftsmerking":     ("DRIFTSMERKING",     str),
    "brutusnummer":      ("DRIFTSMERKING",     str),   # fallback for driftsmerking
}

# Felt vi ønsker å backfille (ikke overskriv eksisterende verdier)
BACKFILL_FELT = [
    "BRUTYPE", "LENGDE_M", "TRAFIKKSTATUS", "DRIFTSMERKING",
]

_num_re = re.compile(r"(\d+(?:[.,]\d+)?)")

def parse_float(x):
    if x is None:
        return None
    m = _num_re.search(str(x))
    return float(m.group(1).replace(",", ".")) if m else None


def parse_alle_eg(tekst: str) -> dict:
    """
    Parser 'Navn: verdi | Navn: verdi | ...' til dict {lowercase_navn: verdi}.
    Bruker lowercase key for robust matching.
    """
    if not tekst:
        return {}
    result = {}
    for token in tekst.split("|"):
        token = token.strip()
        if ":" not in token:
            continue
        key, _, val = token.partition(":")
        result[key.strip().lower()] = val.strip()
    return result


def finn_verdi(parsed: dict, substring: str):
    """Finn første nøkkel i parsed som inneholder substring."""
    for k, v in parsed.items():
        if substring in k:
            return v
    return None


# ------------------------------
# BACKFILL
# ------------------------------
cols = ["OBJECTID", "ALLE_EG"] + BACKFILL_FELT
updated = 0
skipped = 0

print(f"Starter backfill på {FC}...")

with arcpy.da.UpdateCursor(FC, cols) as cur:
    for row in cur:
        oid      = row[0]
        alle_eg  = row[1]

        if not alle_eg:
            skipped += 1
            continue

        parsed = parse_alle_eg(alle_eg)
        endret = False

        # BRUTYPE (idx 2)
        if row[2] is None:
            v = finn_verdi(parsed, "byggverkstype")
            if v:
                row[2] = v.strip()
                endret = True

        # LENGDE_M (idx 3)
        if row[3] is None:
            # Finn "lengde" men ikke "lengste spenn"
            for k, v in parsed.items():
                if k == "lengde":   # eksakt match for å unngå "lengste spenn"
                    f = parse_float(v)
                    if f is not None:
                        row[3] = f
                        endret = True
                    break

        # TRAFIKKSTATUS (idx 4)
        if row[4] is None:
            v = finn_verdi(parsed, "status")
            if v:
                row[4] = v.strip()
                endret = True

        # DRIFTSMERKING (idx 5)
        if row[5] is None:
            v = finn_verdi(parsed, "driftsmerking")
            if v:
                row[5] = v.strip()
                endret = True

        if endret:
            cur.updateRow(row)
            updated += 1

print(f"✅ Backfill ferdig: {updated} rader oppdatert, {skipped} uten ALLE_EG.")
