# 03_korridor_dim_kilde.py
#
# Lager:
# 1) Veg_TillatSegmentert: samme geometri som Veg_TillatProfil, men med propagert min-verdi per VEGLENKESEKV_ID
# 2) Veg_TillatKorridor: Dissolve per VEGLENKESEKV_ID (én linje per id),
#    med DIM_KILDE = "BRU" hvis bru er dimensjonerende (bru <= bk) et sted på lenka, ellers "VEG".

from __future__ import annotations

import arcpy
import os
from typing import Dict, Optional, Iterable, Tuple, List


# ------------------------------
# KONFIG / FELT
# ------------------------------
GDB = r"D:\Conda\Flaskehalser\gdb\nvdb_radata.gdb"
IN_FC = os.path.join(GDB, "Veg_TillatProfil")

OUT_SEG_FC = os.path.join(GDB, "Veg_TillatSegmentert")   # segmenter (samme geometri som IN)
OUT_KORR_FC = os.path.join(GDB, "Veg_TillatKorridor")     # dissolve (én linje per ID)

ID_FIELD = "VEGLENKESEKV_ID"

# Felt fra steg 02
F_TONN = "TILLATT_TONN"      # antatt LONG/DOUBLE (brukes som min)
F_LEN  = "MAKS_LENGDE"       # antatt DOUBLE (valgfritt)
F_HOY  = "MIN_HOYDE"         # antatt DOUBLE (valgfritt)
F_BK   = "BK_VERDI"          # antatt LONG/DOUBLE
F_BRU  = "MIN_BRU_TONN"      # antatt LONG/DOUBLE

# Nye felt i output
F_TONN_PROP = "TONN_PROP"
F_LEN_PROP  = "LEN_PROP"
F_HOY_PROP  = "HOY_PROP"
F_DIM       = "DIM_KILDE"    # "BRU" eller "VEG"
F_PROP      = "PROPAGERT"    # "JA"/"NEI"

DIM_TEXT_LENGTH = 10
FLAG_TEXT_LENGTH = 10


# ------------------------------
# HJELPEFUNKSJONER
# ------------------------------
def ensure_field(fc: str, name: str, ftype: str, *, length: Optional[int] = None) -> None:
    """Opprett felt hvis det ikke finnes fra før."""
    existing = {f.name for f in arcpy.ListFields(fc)}
    if name in existing:
        return
    if length is None:
        arcpy.management.AddField(fc, name, ftype)
    else:
        arcpy.management.AddField(fc, name, ftype, field_length=length)


def dims_kilde_for_segment(bk: Optional[float], bru: Optional[float]) -> Optional[str]:
    """
    Returnerer "BRU" hvis bru er dimensjonerende (bru <= bk),
    ellers "VEG" hvis veg er dimensjonerende.
    Hvis kun én av verdiene finnes, brukes den.
    """
    if bk is None and bru is None:
        return None
    if bk is None:
        return "BRU"
    if bru is None:
        return "VEG"
    return "BRU" if bru <= bk else "VEG"


def validate_inputs() -> Tuple[bool, bool]:
    """Sjekk at inndata finnes og at obligatoriske felt er der. Returnerer (has_len, has_hoy)."""
    if not arcpy.Exists(IN_FC):
        raise RuntimeError(f"Fant ikke input feature class: {IN_FC}")

    fields_in = {f.name for f in arcpy.ListFields(IN_FC)}
    required = [ID_FIELD, F_TONN, F_BK, F_BRU]
    missing = [f for f in required if f not in fields_in]
    if missing:
        raise RuntimeError(f"Mangler felt i {IN_FC}: {missing}")

    has_len = F_LEN in fields_in
    has_hoy = F_HOY in fields_in
    if not has_len:
        arcpy.AddWarning(f"⚠️ Mangler felt {F_LEN} i {IN_FC} (fortsetter uten lengde).")
    if not has_hoy:
        arcpy.AddWarning(f"⚠️ Mangler felt {F_HOY} i {IN_FC} (fortsetter uten høyde).")
    return has_len, has_hoy


# ------------------------------
# KJERNE – STATISTIKK PER LENKE
# ------------------------------
class LinkStats:
    __slots__ = ("tonn", "lengde", "hoyde", "has_bru_dim", "has_any")

    def __init__(self) -> None:
        self.tonn: Optional[float] = None
        self.lengde: Optional[float] = None
        self.hoyde: Optional[float] = None
        self.has_bru_dim: bool = False
        self.has_any: bool = False


def collect_stats(has_len: bool, has_hoy: bool) -> Dict[int, LinkStats]:
    """
    Leser alle segmenter og samler min(tonn|len|hoy) per VEGLENKESEKV_ID,
    samt flagg 'has_bru_dim' dersom BRU er dimensjonerende for minst ett segment.
    """
    read_fields: List[str] = [ID_FIELD, F_TONN, F_BK, F_BRU]
    if has_len:
        read_fields.append(F_LEN)
    if has_hoy:
        read_fields.append(F_HOY)

    # Indekser for rask og robust tilgang
    i_vid = 0
    i_tonn = 1
    i_bk = 2
    i_bru = 3
    i_len = 4 if has_len else None
    i_hoy = (5 if (has_len and has_hoy) else (4 if (not has_len and has_hoy) else None))

    stats: Dict[int, LinkStats] = {}
    with arcpy.da.SearchCursor(IN_FC, read_fields) as cur:
        for row in cur:
            vid = int(row[i_vid])

            s = stats.get(vid)
            if s is None:
                s = stats[vid] = LinkStats()

            s.has_any = True

            tonn = row[i_tonn]
            if tonn is not None:
                s.tonn = tonn if s.tonn is None else min(s.tonn, tonn)

            if i_len is not None:
                lengde = row[i_len]
                if lengde is not None:
                    s.lengde = lengde if s.lengde is None else min(s.lengde, lengde)

            if i_hoy is not None:
                hoyde = row[i_hoy]
                if hoyde is not None:
                    s.hoyde = hoyde if s.hoyde is None else min(s.hoyde, hoyde)

            seg_dim = dims_kilde_for_segment(row[i_bk], row[i_bru])
            if seg_dim == "BRU":
                s.has_bru_dim = True

    arcpy.AddMessage(f"Fant {len(stats)} veglenker med statistikk.")
    return stats


# ------------------------------
# 1) SEGMENT-OUTPUT
# ------------------------------
def build_segment_output(stats: Dict[int, LinkStats], has_len: bool, has_hoy: bool) -> None:
    arcpy.AddMessage("Oppretter segment-output…")
    if arcpy.Exists(OUT_SEG_FC):
        arcpy.management.Delete(OUT_SEG_FC)

    arcpy.management.CopyFeatures(IN_FC, OUT_SEG_FC)

    # Opprett feltene som skal fylles
    ensure_field(OUT_SEG_FC, F_TONN_PROP, "LONG")
    if has_len:
        ensure_field(OUT_SEG_FC, F_LEN_PROP, "DOUBLE")
    if has_hoy:
        ensure_field(OUT_SEG_FC, F_HOY_PROP, "DOUBLE")
    ensure_field(OUT_SEG_FC, F_DIM, "TEXT", length=DIM_TEXT_LENGTH)
    ensure_field(OUT_SEG_FC, F_PROP, "TEXT", length=FLAG_TEXT_LENGTH)

    # Feltrekkefølge i cursor
    upd_fields: List[str] = [ID_FIELD, F_TONN_PROP]
    if has_len:
        upd_fields.append(F_LEN_PROP)
    if has_hoy:
        upd_fields.append(F_HOY_PROP)
    upd_fields += [F_DIM, F_PROP]

    # Indekser
    i_vid = 0
    i_tonn_prop = 1
    next_idx = 2
    i_len_prop = next_idx if has_len else None
    next_idx += 1 if has_len else 0
    i_hoy_prop = next_idx if has_hoy else None
    next_idx += 1 if has_hoy else 0
    i_dim = next_idx
    i_prop = next_idx + 1

    with arcpy.da.UpdateCursor(OUT_SEG_FC, upd_fields) as ucur:
        for row in ucur:
            vid = int(row[i_vid])
            s = stats.get(vid)

            if not s or not s.has_any:
                row[i_prop] = "NEI"
                ucur.updateRow(row)
                continue

            row[i_tonn_prop] = s.tonn
            if i_len_prop is not None:
                row[i_len_prop] = s.lengde
            if i_hoy_prop is not None:
                row[i_hoy_prop] = s.hoyde

            row[i_dim] = "BRU" if s.has_bru_dim else "VEG"
            row[i_prop] = "JA"

            ucur.updateRow(row)

    arcpy.AddMessage("✅ Ferdig segment-output.")


# ------------------------------
# 2) KORRIDOR-OUTPUT (DISSOLVE)
# ------------------------------
def build_corridor_output(stats: Dict[int, LinkStats], has_len: bool, has_hoy: bool) -> None:
    arcpy.AddMessage("Oppretter korridor-output (dissolve per VEGLENKESEKV_ID)…")
    if arcpy.Exists(OUT_KORR_FC):
        arcpy.management.Delete(OUT_KORR_FC)

    # Lag statistikk-liste: MIN for relevante felt
    stat_fields: List[List[str]] = [[F_TONN, "MIN"]]
    if has_len:
        stat_fields.append([F_LEN, "MIN"])
    if has_hoy:
        stat_fields.append([F_HOY, "MIN"])

    # Kjør Dissolve
    arcpy.management.Dissolve(
        in_features=IN_FC,
        out_feature_class=OUT_KORR_FC,
        dissolve_field=ID_FIELD,
        statistics_fields=stat_fields,
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES",  # ✅ riktig parameter og verdi
    )


    # Legg til og sett DIM_KILDE for hver linje
    ensure_field(OUT_KORR_FC, F_DIM, "TEXT", length=DIM_TEXT_LENGTH)

    with arcpy.da.UpdateCursor(OUT_KORR_FC, [ID_FIELD, F_DIM]) as ucur:
        for row in ucur:
            vid = int(row[0])
            s = stats.get(vid)
            row[1] = "BRU" if (s and s.has_bru_dim) else "VEG"
            ucur.updateRow(row)

    arcpy.AddMessage("✅ Ferdig korridor-output.")


# ------------------------------
# HOVEDFLYT
# ------------------------------
def main() -> None:
    arcpy.AddMessage("Leser stats per veglenke (min-verdier + dim-kilde)…")
    with arcpy.EnvManager(overwriteOutput=True):
        has_len, has_hoy = validate_inputs()
        stats = collect_stats(has_len, has_hoy)
        build_segment_output(stats, has_len, has_hoy)
        build_corridor_output(stats, has_len, has_hoy)
    arcpy.AddMessage("✅ Ferdig! Du kan symbolisere korridoren på DIM_KILDE = 'BRU'.")


if __name__ == "__main__":
    main()