#!/usr/bin/env python3
import requests

BASE_URL = "https://nvdbapiles.atlas.vegvesen.no"
ENDPOINT = "/vegnett/api/v4/veglenkesekvenser"
HEADERS = {"X-Client": "MRFK-Kjorelogg-2026", "Accept": "application/json"}

# Test 1: M&R Fv
params = {"fylke": 15, "vegsystemreferanse": "Fv", "antall": 3}
r = requests.get(BASE_URL + ENDPOINT, headers=HEADERS, params=params)
print("=== MØRE & ROMSDAL Fv ===")
print("Status:", r.status_code)
print("Len(objekter):", len(r.json().get("objekter", [])))
print("Første objekt:", r.json().get("objekter", [{}])[0] if r.json().get("objekter") else "INGEN")
print("Geometri-type:", r.json().get("objekter", [{}])[0].get("geometri", {}).get("type") if r.json().get("objekter") else "INGEN")

print("\n=== VESTLAND FV61 ===")
params2 = {"fylke": 46, "kommune": 4649, "vegsystemreferanse": "Fv61", "antall": 3}
r2 = requests.get(BASE_URL + ENDPOINT, headers=HEADERS, params=params2)
print("Status:", r2.status_code)
print("Len(objekter):", len(r2.json().get("objekter", [])))
print("Første:", r2.json().get("objekter", [{}])[0] if r2.json().get("objekter") else "INGEN")

print("\n=== RAW M&R JSON (første 500 tegn) ===")
print(str(r.json())[:500])
