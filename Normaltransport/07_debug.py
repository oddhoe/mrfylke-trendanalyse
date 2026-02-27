# 07_debug.py
# Sjekker GDB, feature classes og feltnavn før 07_blindveg_analyse.py kjøres

import arcpy
import os

GDB = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb"

print("=" * 60)
print("DEBUG – sjekker GDB og innhold")
print("=" * 60)

# ----------------------------------------------------------------
# 1. Sjekk at GDB eksisterer
# ----------------------------------------------------------------
print(f"\n[1] GDB-sti: {GDB}")
if arcpy.Exists(GDB):
    print("    OK – GDB funnet")
else:
    print("    FEIL – GDB finnes ikke! Sjekk stien.")
    raise SystemExit("Avbryter – GDB ikke funnet.")

# ----------------------------------------------------------------
# 2. List alle feature classes i GDB
# ----------------------------------------------------------------
print("\n[2] Feature classes i GDB:")
arcpy.env.workspace = GDB
fcs = arcpy.ListFeatureClasses()
if fcs:
    for fc in sorted(fcs):
        count = int(arcpy.management.GetCount(fc)[0])
        print(f"    {fc:<50} ({count} rader)")
else:
    print("    ADVARSEL – ingen feature classes funnet i GDB!")

# ----------------------------------------------------------------
# 3. Sjekk Vegnett spesifikt
# ----------------------------------------------------------------
print("\n[3] Sjekker 'Vegnett':")
FC_VEGNETT = os.path.join(GDB, "Vegnett")
if arcpy.Exists(FC_VEGNETT):
    print("    OK – Vegnett funnet")
    print("    Felter:")
    for f in arcpy.ListFields(FC_VEGNETT):
        print(f"      {f.name:<40} type={f.type:<10} length={f.length}")
    # Vis første rad som eksempel
    print("    Første rad (eksempel):")
    fields = [f.name for f in arcpy.ListFields(FC_VEGNETT) if f.type != "Geometry"]
    with arcpy.da.SearchCursor(FC_VEGNETT, fields[:6]) as cur:
        for row in cur:
            for fname, val in zip(fields[:6], row):
                print(f"      {fname}: {val}")
            break
else:
    print("    FEIL – 'Vegnett' finnes ikke i GDB!")
    print("    Tilgjengelige navn (se liste over) – sjekk om det heter noe annet.")

# ----------------------------------------------------------------
# 4. Sjekk FlaskehalserBK904Normal50t195m45m
# ----------------------------------------------------------------
print("\n[4] Sjekker 'FlaskehalserBK904Normal50t195m45m':")
FC_FLASKEHALSER = os.path.join(GDB, "FlaskehalserBK904Normal50t195m45m")
if arcpy.Exists(FC_FLASKEHALSER):
    print("    OK – flaskehalslaget funnet")
    print("    Felter:")
    for f in arcpy.ListFields(FC_FLASKEHALSER):
        print(f"      {f.name:<40} type={f.type:<10}")
else:
    print("    INFO – flaskehalslaget finnes ikke (steg 04 ikke kjørt ennå – OK)")

# ----------------------------------------------------------------
# 5. Sjekk networkx
# ----------------------------------------------------------------
print("\n[5] Sjekker networkx:")
try:
    import networkx as nx
    print(f"    OK – networkx versjon {nx.__version__}")
except ImportError:
    print("    FEIL – networkx ikke installert!")
    print("    Kjør:  conda install -c conda-forge networkx")

print("\n" + "=" * 60)
print("Debug ferdig – se over for eventuelle FEIL eller ADVARSEL")
print("=" * 60)
