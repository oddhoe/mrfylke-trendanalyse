# debug_spes_egenskaper.py
# Skriver ut alle egenskapsnavn + verdier for BK 904-objekter med Spes-begrensning
import requests

FYLKE       = 15
NVDB_API    = "https://nvdbapiles.atlas.vegvesen.no"
VEGOBJ_API  = f"{NVDB_API}/vegobjekter/api/v4"
HEADERS     = {
    "X-Client": "debug_spes",
    "Accept":   "application/vnd.vegvesen.nvdb-v3+json",
}

params = {
    "fylke":             FYLKE,
    "vegsystemreferanse": "F",
    "antall":            5,
    "inkluder":          "egenskaper,lokasjon",
    "alle_versjoner":    "false",
    # Filter: Maks vogntoglengde (10913) = Spesiell begrensning (18256)
    "egenskap":          "10913=18256",
}

r = requests.get(f"{VEGOBJ_API}/vegobjekter/904", headers=HEADERS, params=params, timeout=30)
data = r.json()

for o in data.get("objekter", []):
    print(f"\n=== NVDB ID: {o['id']} ===")
    for e in o.get("egenskaper", []):
        print(f"  id={e.get('id'):>6}  navn='{e.get('navn')}'  verdi='{e.get('verdi')}'")
