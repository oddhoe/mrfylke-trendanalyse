# fields.py
from __future__ import annotations
from typing import Optional, Literal
import arcpy

FieldType = Literal[
    "SHORT",
    "LONG",
    "BIGINTEGER",
    "FLOAT",
    "DOUBLE",
    "TEXT",
    "DATE",
    "DATEHIGHPRECISION",
    "DATEONLY",
    "TIMEONLY",
    "TIMESTAMPOFFSET",
    "BLOB",
    "GUID",
    "RASTER",
]

def ensure_field(fc: str, name: str, ftype: FieldType, length: Optional[int] = None) -> None:
    """Opprett felt hvis det ikke finnes (Pylance‑ren signatur)."""
    existing = {f.name for f in arcpy.ListFields(fc)}
    if name in existing:
        return
    arcpy.management.AddField(fc, name, ftype, field_length=length)