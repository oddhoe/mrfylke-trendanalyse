# debug_bru_272702056.py
import requests

NVDB_API = "https://nvdbapiles.atlas.vegvesen.no"
HEADERS  = {
    "X-Client": "debug",
    "Accept":   "application/vnd.vegvesen.nvdb-v3+json",
}

r = requests.get(
    f"{NVDB_API}/vegobjekter/api/v4/vegobjekter/60/272702056",
    headers=HEADERS,
    params={"inkluder": "egenskaper"},
    timeout=30,
)

for e in r.json().get("egenskaper", []):
    print(f"  id={e.get('id'):>6}  navn='{e.get('navn')}'  verdi='{e.get('verdi')}'")
