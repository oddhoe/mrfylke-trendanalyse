# 06_hent_ekstra_info_FIXED.py
# ✅ FIXED: Henter nå VEGLENKESEKV_ID og posisjon slik at Skript 02 finner den!

import arcpy
import requests
import os

arcpy.env.overwriteOutput = True

# --- KONFIGURASJON ---

GDB = r"D:\Conda\Flaskehalser\gdb\nvdb_radata.gdb"
FYLKE = 15  # Møre og Romsdal
OBJEKTTYPE = 591  # Høydebegrensning
VEG_KAT = "FV"  # Fylkesvei

BASE_URL = f"https://nvdbapiles.atlas.vegvesen.no/vegobjekter/api/v4/vegobjekter/{OBJEKTTYPE}"

HEADERS = {
    "X-Client": "FlaskehalsAnalyse_Script",
    "Accept": "application/vnd.vegvesen.nvdb-v3+json"
}

START_PARAMS = {
    "fylke": FYLKE,
    "vegsystemreferanse": VEG_KAT,
    "inkluder": "egenskaper,lokasjon,geometri",
    "srid": 5973,
    "alle_versjoner": "false"
}

OUT_FC = os.path.join(GDB, f"Hoydebegrensning_{OBJEKTTYPE}")

def hent_alle_objekter():
    url = BASE_URL
    current_params = START_PARAMS
    objekter = []
    
    print(f"Starter nedlasting av objekttype {OBJEKTTYPE}...")
    
    while url:
        if len(objekter) > 0 and len(objekter) % 100 == 0:
            print(f" ... {len(objekter)} lastet ned.")
        
        try:
            r = requests.get(url, params=current_params, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                print(f"Feil: {r.status_code}")
                break
            
            data = r.json()
            nye = data.get("objekter", [])
            
            if not nye: break
            
            objekter.extend(nye)
            
            neste = data.get("metadata", {}).get("neste", {})
            url = neste.get("href")
            current_params = {}
        
        except Exception as e:
            print(f"Error: {e}")
            break
    
    print(f"Totalt {len(objekter)} objekter funnet.")
    return objekter

def lagre_til_gdb(objekter):
    if not objekter: return
    
    print(f"Lagrer til {OUT_FC}...")
    if arcpy.Exists(OUT_FC):
        arcpy.management.Delete(OUT_FC)
    
    sr = arcpy.SpatialReference(25833)
    arcpy.management.CreateFeatureclass(os.path.dirname(OUT_FC), os.path.basename(OUT_FC), "POINT", spatial_reference=sr)
    
    # Felter
    arcpy.management.AddField(OUT_FC, "NVDB_ID", "LONG")
    arcpy.management.AddField(OUT_FC, "SKILTET_HOYDE", "DOUBLE")
    arcpy.management.AddField(OUT_FC, "TYPE_HINDER", "TEXT", field_length=50)
    
    # ✅ VIKTIG: Legger til koblingsnøkler for Skript 02
    arcpy.management.AddField(OUT_FC, "VEGLENKESEKV_ID", "LONG")
    arcpy.management.AddField(OUT_FC, "STARTPOS", "DOUBLE")
    arcpy.management.AddField(OUT_FC, "SLUTTPOS", "DOUBLE")
    
    cols = ["SHAPE@", "NVDB_ID", "SKILTET_HOYDE", "TYPE_HINDER", "VEGLENKESEKV_ID", "STARTPOS", "SLUTTPOS"]
    
    with arcpy.da.InsertCursor(OUT_FC, cols) as cur:
        count = 0
        for o in objekter:
            nvdb_id = o["id"]
            
            # 1. Hent Høyde
            høyde = next((e["verdi"] for e in o.get("egenskaper", []) if e["id"] == 5277), None)
            if not høyde: continue
            
            # 2. Hent Type
            type_hinder = next((e["verdi"] for e in o.get("egenskaper", []) if e["id"] == 5270), "Ukjent")
            
            # 3. Hent Posisjon (ID)
            stedfestinger = o.get("lokasjon", {}).get("stedfestinger", [])
            vid = None
            startpos = 0.0
            sluttpos = 0.0
            
            # Vi tar den første gyldige stedfestingen på vegnettet
            for s in stedfestinger:
                if s.get("veglenkesekvensid"):
                    vid = int(s["veglenkesekvensid"])
                    startpos = float(s.get("startposisjon", 0))
                    sluttpos = float(s.get("sluttposisjon", startpos)) # Punkt har ofte start=slutt
                    break
            
            if not vid: continue # Hopp over hvis vi ikke finner veglenke-kobling
            
            # 4. Geometri
            wkt = o.get("geometri", {}).get("wkt")
            if not wkt: continue
            
            try:
                pt_geom = arcpy.FromWKT(wkt, sr)
                if pt_geom.type != 'point': pt_geom = pt_geom.centroid
                
                cur.insertRow((pt_geom, nvdb_id, høyde, type_hinder, vid, startpos, sluttpos))
                count += 1
            except:
                pass
                
    print(f"✅ Suksess! Lagret {count} høydebegrensninger med ID-kobling.")

if __name__ == "__main__":
    objs = hent_alle_objekter()
    lagre_til_gdb(objs)
