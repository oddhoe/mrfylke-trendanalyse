import json, pandas as pd, numpy as np

with open("Alle-nedboyninger-2017-2026.geojson", "r", encoding="utf-8") as f:
    data = json.load(f)

rows = []
for feat in data["features"]:
    p = feat["properties"].copy()
    p["lon"] = feat["geometry"]["coordinates"][0]
    p["lat"] = feat["geometry"]["coordinates"][1]
    rows.append(p)

df = pd.DataFrame(rows)
df["meter"] = pd.to_numeric(df["meter"], errors="coerce")
df["tons"]  = pd.to_numeric(df["tons"],  errors="coerce")

# Geografisk nærhet: rund til 10m-ruter
df["lon_r"] = (df["lon"] / 10).round() * 10
df["lat_r"] = (df["lat"] / 10).round() * 10

grp = df.groupby(["fv", "lon_r", "lat_r"]).agg(
    antall_malinger  = ("fid",          "count"),
    antall_datoer    = ("measure_date", "nunique"),
    datoer           = ("measure_date", lambda x: "|".join(sorted(x.unique()))),
    meter_verdier    = ("meter",        lambda x: "|".join(map(str, sorted(x.unique())))),
    tons_min         = ("tons",         "min"),
    tons_max         = ("tons",         "max"),
    lon_snitt        = ("lon",          "mean"),
    lat_snitt        = ("lat",          "mean"),
).reset_index()

# Behold bare steder med >1 måling (potensiell dobbel retning)
resultat = grp[grp["antall_malinger"] > 1].sort_values(
    ["fv", "antall_malinger"], ascending=[True, False]
)

print(f"Totalt: {len(resultat)} steder med >1 måling")
print(resultat.groupby("fv")["antall_malinger"].sum().sort_values(ascending=False).head(20))

resultat.to_csv("duplikat_malinger_begge_retninger.csv", index=False)
print("Lagret: duplikat_malinger_begge_retninger.csv")
