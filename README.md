# mrfylke-trendanalyse

## Oppsett
- ArcGIS Pro Python (arcgispro-py3)
- `config.py` peker til riktig GDB

## Kjørerekkefølge
1. `01_nvdb_to_gdb.py` (bygger basislag)
2. `06_hent_ekstra_info.py` (høyde < 4.5 m)
3. `02_bygg_tillat_profil.py` (profil med DIM_KILDE)
4. `03_korridor_dim_kilde.py` (segment + korridor)

## Tips
- Endre kjøretøykrav i `config.py`
- Ikke sjekk inn .gdb – bygges med 01/06