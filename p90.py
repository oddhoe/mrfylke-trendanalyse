import pandas as pd
import numpy as np
import arcpy
import os
import glob
import re
from io import StringIO

# ================= INNSTILLINGER =================
input_folder = r"G:\Test\Prosjekt_2025\Vegtilstandskart\data\SDV_NOR"
trafikk_fil = r"G:\Test\Prosjekt_2025\Vestland\Trafikkmengde_NOR.csv"

output_gdb_path = r"G:\Test\Prosjekt_2025\Vestland"
output_gdb_name = "Kartdata_nfk.gdb"
full_gdb_path = os.path.join(output_gdb_path, output_gdb_name)

output_fc_name = "Spor_Persentiler_1000m"
parsell_lengde = 1000  # meter

spatial_ref = arcpy.SpatialReference(25833)
# =================================================

def ensure_gdb_exists():
    if not arcpy.Exists(full_gdb_path):
        print(f"Oppretter ny File Geodatabase: {full_gdb_path}")
        if not os.path.exists(output_gdb_path):
            os.makedirs(output_gdb_path)
        arcpy.management.CreateFileGDB(output_gdb_path, output_gdb_name)

def read_trafikk_data(csv_path):
    """Leser NVDB ÅDT-data og returnerer dict med vegnummer som nøkkel."""
    print(f"Leser trafikkdata fra: {os.path.basename(csv_path)}")
    try:
        # Les CSV med riktig separator
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8", on_bad_lines='skip')
        
        # Rens kolonnenavn (fjern fnutter og whitespace)
        df.columns = [c.replace('"', '').strip() for c in df.columns]
        
        # Sjekk hvilke kolonner vi faktisk har
        # print("Kolonner i CSV:", df.columns.tolist()) 
        
        adt_map = {}
        for _, row in df.iterrows():
            try:
                # Bruk VSR.VEGNUMMER (det faktiske vegnummeret, f.eks. 585)
                # Hvis VSR.VEGNUMMER mangler, prøv OBJ.VEGO som fallback, men det er risikabelt
                if 'VSR.VEGNUMMER' in row and pd.notna(row['VSR.VEGNUMMER']):
                    vegnr = int(row['VSR.VEGNUMMER'])
                else:
                    continue 

                adt = int(row['EGS.ÅDT, TOTAL.4623'])
                
                if vegnr not in adt_map:
                    adt_map[vegnr] = []
                adt_map[vegnr].append(adt)
            except (ValueError, KeyError):
                continue
                
        # Beregn snitt eller bruk maks ÅDT for vegnummeret
        # For sikkerhets skyld bruker vi MAKS verdi her for å ikke undervurdere trafikken
        adt_final = {veg: int(np.max(values)) for veg, values in adt_map.items()}
        
        print(f"Fant ÅDT-data for {len(adt_final)} unike vegnummer.")
        return adt_final
    except Exception as e:
        print(f"Feil ved lesing av trafikkfil: {e}")
        return {}


def extract_vegnummer_from_filename(filename):
    """Henter ut vegnummer fra SDV-filnavn, f.eks. 'FV585' -> 585."""
    match = re.search(r'[FR]V(\d+)', filename)
    if match:
        return int(match.group(1))
    return None

def get_trafikk_niva(vegnummer, adt_map):
    """Returnerer 'Høy' eller 'Lav' basert på ÅDT for vegnummeret."""
    if vegnummer in adt_map:
        adt = adt_map[vegnummer]
        return "Høy" if adt > 5000 else "Lav"
    else:
        # Fallback hvis ikke funnet (konservativt estimat)
        print(f"Advarsel: Ingen ÅDT funnet for vegnummer {vegnummer}. Bruker 'Høy' som sikkerhet.")
        return "Høy"

def get_tg_spor(value, trafikk):
    if pd.isna(value): return None
    if trafikk == "Lav":
        if value <= 15: return 0
        elif value <= 25: return 1
        elif value <= 35: return 2
        else: return 3
    else:
        if value <= 10: return 0
        elif value <= 20: return 1
        elif value <= 30: return 2
        else: return 3

def get_tg_iri(value, trafikk):
    if pd.isna(value): return None
    if trafikk == "Lav":
        if value <= 2.5: return 0
        elif value <= 5.0: return 1
        elif value <= 6.5: return 2
        else: return 3
    else:
        if value <= 1.5: return 0
        elif value <= 4.0: return 1
        elif value <= 5.5: return 2
        else: return 3

def read_sdv(file_path):
    encodings = ["windows-1252", "latin1", "utf-8"]
    lines = None
    for enc in encodings:
        try:
            with open(file_path, "r", encoding=enc) as f: 
                lines = f.readlines()
            break
        except UnicodeDecodeError: 
            continue
            
    if lines is None: return None

    header_idx = None
    for i, line in enumerate(lines):
        if "Utkjørt meter [m]" in line and "Spordybde [mm]" in line:
            header_idx = i
            break
            
    if header_idx is None: return None

    try:
        df = pd.read_csv(
            StringIO("".join(lines[header_idx:])),
            sep=";", decimal=",", engine="python",
            na_values=["NaN", "_", ""], on_bad_lines="skip"
        )
    except: 
        return None

    if "" in df.columns: 
        df = df.drop(columns=[""])
    df.columns = [c.strip() for c in df.columns]
    return df

def calculate_percentiles():
    ensure_gdb_exists()
    
    # Last trafikkdata som dict
    adt_map = read_trafikk_data(trafikk_fil)
    
    print("Starter analyse av måledata...")
    files = glob.glob(os.path.join(input_folder, "*.sdv"))
    if not files: return

    all_data = []
    for f in files:
        print(f"Leser: {os.path.basename(f)}")
        df = read_sdv(f)
        if df is not None and not df.empty:
            df['Kildefil'] = os.path.basename(f)
            
            # Hent vegnummer fra filnavnet
            vegnr = extract_vegnummer_from_filename(os.path.basename(f))
            df['Vegnummer'] = vegnr
            
            all_data.append(df)

    if not all_data: return
    full_df = pd.concat(all_data, ignore_index=True)

    numeric_cols = ['Utkjørt meter [m]', 'Spordybde [mm]', 'Sone 33V N [m]', 'Sone 33V Ø [m]']
    iri_col = 'Alfred IRI [mm/m]' if 'Alfred IRI [mm/m]' in full_df.columns else None
    
    for col in numeric_cols:
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce')
    if iri_col: 
        full_df[iri_col] = pd.to_numeric(full_df[iri_col], errors='coerce')

    full_df = full_df.dropna(subset=numeric_cols)

    # Gruppering
    full_df['Parsell_ID'] = (full_df['Utkjørt meter [m]'] // parsell_lengde).astype(int)
    grouped = full_df.groupby(['Kildefil', 'Vegnummer', 'Fra felt []', 'Parsell_ID'])

    result_rows = []
    print(f"\nBeregner persentiler per {parsell_lengde} meter...")

    for (filnavn, vegnr, felt, parsell_id), group in grouped:
        if group.empty: continue
        group = group.sort_values('Utkjørt meter [m]')
        
        # Hent trafikknivå basert på vegnummer
        trafikk_niva = get_trafikk_niva(vegnr, adt_map)

        spor_vals = group['Spordybde [mm]'].values
        if len(spor_vals) == 0: continue
        
        spor_p90 = np.percentile(spor_vals, 90)
        tg_spor = get_tg_spor(spor_p90, trafikk_niva)
        
        iri_p90 = 0.0
        tg_iri = 0
        if iri_col:
            iri_vals = group[iri_col].dropna().values
            if len(iri_vals) > 0:
                iri_p90 = np.percentile(iri_vals, 90)
                tg_iri = get_tg_iri(iri_p90, trafikk_niva)

        points = [arcpy.Point(r['Sone 33V Ø [m]'], r['Sone 33V N [m]']) for _, r in group.iterrows()]
        if len(points) < 2: continue

        line_geo = arcpy.Polyline(arcpy.Array(points), spatial_ref)
        
        result_rows.append({
            'Kildefil': filnavn,
            'Vegnummer': int(vegnr) if vegnr else -1,
            'Felt': str(felt),
            'Parsell_Start': float(group['Utkjørt meter [m]'].min()),
            'Parsell_Slutt': float(group['Utkjørt meter [m]'].max()),
            'Trafikk_Niva': trafikk_niva,
            'Spor_P90': float(spor_p90),
            'TG_Spor': int(tg_spor) if tg_spor is not None else -1,
            'IRI_P90': float(iri_p90),
            'TG_IRI': int(tg_iri) if tg_iri is not None else -1,
            'SHAPE@': line_geo
        })

    # Lagre
    out_path = os.path.join(full_gdb_path, output_fc_name)
    if arcpy.Exists(out_path): 
        print("Sletter gammelt lag...")
        arcpy.management.Delete(out_path)
    
    print(f"Lagrer {len(result_rows)} parseller...")
    arcpy.management.CreateFeatureclass(full_gdb_path, output_fc_name, "POLYLINE", spatial_reference=spatial_ref)
    
    fields = [
        ('Kildefil', 'TEXT'), ('Vegnummer', 'LONG'), ('Felt', 'TEXT'),
        ('Parsell_Start', 'DOUBLE'), ('Parsell_Slutt', 'DOUBLE'),
        ('Trafikk_Niva', 'TEXT'),
        ('Spor_P90', 'DOUBLE'), ('TG_Spor', 'SHORT'),
        ('IRI_P90', 'DOUBLE'), ('TG_IRI', 'SHORT')
    ]
    
    for fn, ft in fields: 
        arcpy.management.AddField(out_path, fn, ft)
    
    cursor_fields = ['SHAPE@'] + [f[0] for f in fields]
    with arcpy.da.InsertCursor(out_path, cursor_fields) as cursor:
        for row in result_rows:
            cursor.insertRow([row[f] for f in cursor_fields])
            
    print("Ferdig! ÅDT er nå koblet riktig basert på vegnummer.")

if __name__ == '__main__':
    calculate_percentiles()
