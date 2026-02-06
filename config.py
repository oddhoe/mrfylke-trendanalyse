# config.py
from __future__ import annotations

# ÉN sannhet for hvor GDB ligger
GDB = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\gdb\nvdb_radata.gdb"

# NVDB / SRID / fylke
FYLKE = 15
SRID = 5973

# Kjøretøyprofiler – kan utvides (STANDARD, TOMMER, osv.)
KJORETOY_TOMMER = {
    "NAVN": "TOMMER",
    "TONN": 60.0,
    "LENGDE": 24.0,
    "HOYDE": 4.2,
}