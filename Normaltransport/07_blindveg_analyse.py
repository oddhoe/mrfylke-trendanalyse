# 07_blindveg_analyse.py
# Bruker Bruksklasse_904 som vegnett-kilde.
# Et segment er RØDT hvis det er en "bro" i grafen (nx.bridges),
# dvs. fjerning av segmentet ville økt antall komponenter → ingen omkjøring.
# VEGREF og KOMMUNE hentes fra Vegnett via VID-oppslag.

import arcpy
import os
import networkx as nx

arcpy.env.overwriteOutput = True

GDB             = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"
FC_BK           = os.path.join(GDB, "Bruksklasse_904")
FC_VEGNETT      = os.path.join(GDB, "Vegnett")
FC_FLASKEHALSER = os.path.join(GDB, "Flaskehalser_BK904_Normal_50t_19_5m_4_5m")
OUTFC_STATUS    = os.path.join(GDB, "Vegnett_MedOmkjoringStatus")
OUTFC_UTEN      = os.path.join(GDB, "Vegnett_UtenOmkjoring")

SNAP_DECIMALS = 1

def round_coord(c, decimals=SNAP_DECIMALS):
    f = 10 ** decimals
    return (round(c[0] * f) / f, round(c[1] * f) / f)

# ----------------------------------------------------------------
# Steg 1: Hent VEGREF og KOMMUNE fra Vegnett via VID-oppslag
# ----------------------------------------------------------------
print("=" * 60)
print("Steg 1: Henter VEGREF og KOMMUNE fra Vegnett...")
print("=" * 60)

vid_info = {}  # vid_int -> (vegref, kommune)
with arcpy.da.SearchCursor(FC_VEGNETT, ["VEGLENKESEKV_ID", "VEGREF", "KOMMUNE"]) as cur:
    for row in cur:
        vid = row[0]
        if vid is None:
            continue
        vid_int = int(vid)
        if vid_int not in vid_info:
            vid_info[vid_int] = (row[1] or "", row[2] or "")

print(f"  VID-oppslag bygget: {len(vid_info)} unike VID-er")

# ----------------------------------------------------------------
# Steg 2: Les Bruksklasse_904 og bygg nettverksgraf
# ----------------------------------------------------------------
print()
print("=" * 60)
print("Steg 2: Leser Bruksklasse_904 og bygger nettverksgraf...")
print("=" * 60)

G           = nx.Graph()
alle_vider  = []
kant_til_id = {}   # (n_start, n_end) -> vid_int  (for brooppslag)
sett_kanter = set()

count_lest = 0
skip_annet = 0

read_cols = ["VEGLENKESEKV_ID", "SHAPE@", "STARTPOS", "SLUTTPOS"]

with arcpy.da.SearchCursor(FC_BK, read_cols) as cur:
    for row in cur:
        vid  = row[0]
        geom = row[1]
        s0   = row[2]
        s1   = row[3]

        if vid is None or geom is None:
            skip_annet += 1
            continue

        vid_int = int(vid)
        pts = [pt for part in geom for pt in part if pt is not None]
        if len(pts) < 2:
            skip_annet += 1
            continue

        n_start  = round_coord((pts[0].X,  pts[0].Y))
        n_end    = round_coord((pts[-1].X, pts[-1].Y))
        kant_key = (min(n_start, n_end), max(n_start, n_end))

        vegref, kommune = vid_info.get(vid_int, ("", ""))

        if kant_key not in sett_kanter:
            G.add_edge(n_start, n_end, vid=vid_int)
            kant_til_id[kant_key] = vid_int
            sett_kanter.add(kant_key)

        alle_vider.append((vid_int, geom, s0, s1, vegref, kommune, n_start, n_end))
        count_lest += 1

print(f"  Lest inn:    {count_lest}")
print(f"  Hoppet over: {skip_annet}")
print(f"  Graf: {G.number_of_nodes()} noder, {G.number_of_edges()} kanter")

# ----------------------------------------------------------------
# Steg 3: Finn broer (segmenter uten omkjøring)
# En bro = fjerning av kanten øker antall komponenter
# = ingen alternativ rute = RØD
# ----------------------------------------------------------------
print()
print("=" * 60)
print("Steg 3: Beregner broer (nx.bridges)...")
print("=" * 60)

broer = set()
for u, v in nx.bridges(G):
    kant = (min(u, v), max(u, v))
    broer.add(kant)

print(f"  Broer (rød):        {len(broer)}")
print(f"  Ikke-broer (grønn): {G.number_of_edges() - len(broer)}")

# ----------------------------------------------------------------
# Steg 4: Bygg kant-status
# ----------------------------------------------------------------
kant_status = {}
for vid, geom, s0, s1, vegref, kommune, n_start, n_end in alle_vider:
    kant = (min(n_start, n_end), max(n_start, n_end))
    # Grønn = ikke bro (har omkjøring)
    kant_status[kant] = kant not in broer

# ----------------------------------------------------------------
# Steg 5: Lag output – Vegnett_MedOmkjoringStatus
# ----------------------------------------------------------------
print()
print("=" * 60)
print("Steg 5: Oppretter Vegnett_MedOmkjoringStatus...")
print("=" * 60)

sr = arcpy.Describe(FC_BK).spatialReference
if arcpy.Exists(OUTFC_STATUS):
    arcpy.management.Delete(OUTFC_STATUS)

arcpy.management.CreateFeatureclass(
    os.path.dirname(OUTFC_STATUS),
    os.path.basename(OUTFC_STATUS),
    "POLYLINE", spatial_reference=sr
)

for fname, ftype, flen in [
    ("VEGLENKESEKV_ID", "LONG",   None),
    ("STARTPOS",        "DOUBLE", None),
    ("SLUTTPOS",        "DOUBLE", None),
    ("UTEN_OMKJORING",  "TEXT",   5),
    ("GRAD_START",      "SHORT",  None),
    ("GRAD_SLUTT",      "SHORT",  None),
    ("LENGDE",          "DOUBLE", None),
    ("VEGREF",          "TEXT",   50),
    ("KOMMUNE",         "TEXT",   60),
]:
    if flen:
        arcpy.management.AddField(OUTFC_STATUS, fname, ftype, field_length=flen)
    else:
        arcpy.management.AddField(OUTFC_STATUS, fname, ftype)

node_grad  = dict(G.degree())
write_cols = [
    "SHAPE@", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS",
    "UTEN_OMKJORING", "GRAD_START", "GRAD_SLUTT",
    "LENGDE", "VEGREF", "KOMMUNE"
]

count_ja = count_nei = 0
with arcpy.da.InsertCursor(OUTFC_STATUS, write_cols) as icur:
    for vid, geom, s0, s1, vegref, kommune, n_start, n_end in alle_vider:
        kant   = (min(n_start, n_end), max(n_start, n_end))
        har_omkjoring = kant_status.get(kant, False)
        status = "NEI" if har_omkjoring else "JA"
        icur.insertRow([
            geom, vid, s0, s1, status,
            node_grad.get(n_start, 0),
            node_grad.get(n_end,   0),
            geom.length, vegref, kommune
        ])
        if status == "JA": count_ja  += 1
        else:               count_nei += 1

print(f"  Skrevet {count_ja + count_nei} segmenter")
print(f"    JA  (rød):   {count_ja}")
print(f"    NEI (grønn): {count_nei}")

# ----------------------------------------------------------------
# Steg 6: Eksporter kun veger uten omkjøring
# ----------------------------------------------------------------
print()
print("=" * 60)
print("Steg 6: Eksporterer Vegnett_UtenOmkjoring...")
print("=" * 60)

arcpy.conversion.ExportFeatures(
    OUTFC_STATUS, OUTFC_UTEN,
    where_clause="UTEN_OMKJORING = 'JA'"
)
print(f"  {int(arcpy.management.GetCount(OUTFC_UTEN)[0])} segmenter eksportert")

# ----------------------------------------------------------------
# Steg 7: Oppdater flaskehalslaget
# ----------------------------------------------------------------
print()
print("=" * 60)
print("Steg 7: Sjekker flaskehalslaget...")
print("=" * 60)

if arcpy.Exists(FC_FLASKEHALSER):
    print("  Funnet – oppdaterer UTEN_OMKJORING...")
    existing = [f.name for f in arcpy.ListFields(FC_FLASKEHALSER)]
    if "UTEN_OMKJORING" not in existing:
        arcpy.management.AddField(FC_FLASKEHALSER, "UTEN_OMKJORING", "TEXT", field_length=5)

    status_lookup = {}
    with arcpy.da.SearchCursor(OUTFC_STATUS, ["VEGLENKESEKV_ID", "UTEN_OMKJORING"]) as cur:
        for vid, status in cur:
            status_lookup[vid] = status

    fl_fields = [f.name for f in arcpy.ListFields(FC_FLASKEHALSER)]
    fl_id = "VEGLENKESEKV_ID" if "VEGLENKESEKV_ID" in fl_fields else "VEGLENKESEKVID"

    oppdatert = 0
    with arcpy.da.UpdateCursor(FC_FLASKEHALSER, [fl_id, "UTEN_OMKJORING"]) as ucur:
        for row in ucur:
            row[1] = status_lookup.get(row[0], None)
            ucur.updateRow(row)
            oppdatert += 1
    print(f"  {oppdatert} rader oppdatert")
else:
    print("  Flaskehalslaget finnes ikke – hopper over.")

# ----------------------------------------------------------------
# Ferdig
# ----------------------------------------------------------------
print()
print("=" * 60)
print("FERDIG!")
print("=" * 60)
print(f"  Alle veger med status:    {OUTFC_STATUS}")
print(f"  Kun veger uten omkjøring: {OUTFC_UTEN}")
print()
print("  Symboliser på UTEN_OMKJORING:")
print("    JA  = rød   (ingen omkjøring – bro i grafen)")
print("    NEI = grønn (omkjøring mulig – del av sykel)")
