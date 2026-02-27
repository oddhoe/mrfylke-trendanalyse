# 07_debug_csv.py – sjekk faktiske feltnavn og verdier i vegnett.csv

import csv

CSV_FERGE = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\vegnett_v2.csv"

with open(CSV_FERGE, encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f, delimiter=";")

    # Skriv ut alle feltnavn
    print("FELTNAVN I CSV:")
    for felt in reader.fieldnames:
        print(f"  '{felt}'")

    print()

    # Skriv ut unike verdier for relevante felt
    netttype_verdier  = set()
    typeveg_verdier   = set()
    adskilte_verdier  = set()
    arm_verdier       = set()
    delstr_verdier    = set()

    for row in reader:
        netttype_verdier.add( (row.get("NET.TYPE")            or "").strip())
        typeveg_verdier.add(  (row.get("NET.TYPEVEG")         or "").strip())
        adskilte_verdier.add( (row.get("VSR.ADSKILTE_LØP")    or "").strip())
        arm_verdier.add(      (row.get("VSR.STREKNING-ARM")   or "").strip())
        try:
            delstr_verdier.add(int((row.get("VSR.DELSTREKNING") or "0").strip()))
        except ValueError:
            pass

print("NET.TYPE unike verdier:")
for v in sorted(netttype_verdier):
    print(f"  '{v}'")

print()
print("NET.TYPEVEG unike verdier:")
for v in sorted(typeveg_verdier):
    print(f"  '{v}'")

print()
print("VSR.ADSKILTE_LØP unike verdier:")
for v in sorted(adskilte_verdier):
    print(f"  '{v}'")

print()
print("VSR.STREKNING-ARM unike verdier:")
for v in sorted(arm_verdier):
    print(f"  '{v}'")

print()
print("VSR.DELSTREKNING unike verdier:")
for v in sorted(delstr_verdier):
    print(f"  {v}")

print()

# Vis eksempel på ferge-rader
print("EKSEMPEL – ferge-rader (NET.TYPEVEG = Bilferje):")
with open(CSV_FERGE, encoding="utf-8-sig", newline="") as f2:
    reader2 = csv.DictReader(f2, delimiter=";")
    count = 0
    for row in reader2:
        if (row.get("NET.TYPEVEG") or "").strip() == "Bilferje":
            print(f"  VID={row.get('NET.VEGLENKESEKVENSID')}  "
                  f"NET.TYPE={row.get('NET.TYPE')}  "
                  f"VEGREF={row.get('VSR.VEGSYSTEMREFERANSE')}  "
                  f"STARTNODE={row.get('NET.STARTNODE')}  "
                  f"SLUTTNODE={row.get('NET.SLUTTNODE')}")
            count += 1
            if count >= 5:
                break

# Vis eksempel på konnektering-kandidater
print()
print("EKSEMPEL – mulige konnekteringer (NET.TYPE != HOVED):")
with open(CSV_FERGE, encoding="utf-8-sig", newline="") as f3:
    reader3 = csv.DictReader(f3, delimiter=";")
    count = 0
    for row in reader3:
        ntype = (row.get("NET.TYPE") or "").strip().upper()
        if ntype not in ("HOVED", ""):
            print(f"  VID={row.get('NET.VEGLENKESEKVENSID')}  "
                  f"NET.TYPE='{ntype}'  "
                  f"VEGREF={row.get('VSR.VEGSYSTEMREFERANSE')}")
            count += 1
            if count >= 10:
                break
