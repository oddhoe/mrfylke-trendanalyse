#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import arcpy
import os

# ------------------------------------------------------------
# SETT STI HER
# ------------------------------------------------------------

GPKG_PATH = r"G:\Test\2026\Ansvarsområder.gpkg"

# ------------------------------------------------------------
# START
# ------------------------------------------------------------

if not os.path.exists(GPKG_PATH):
    raise RuntimeError(f"Fant ikke fil: {GPKG_PATH}")

print("\n=== INSPEKSJON AV GEOPACKAGE ===")
print(f"Fil: {GPKG_PATH}")

arcpy.env.workspace = GPKG_PATH

fcs = arcpy.ListFeatureClasses()

if not fcs:
    print("\nIngen feature classes funnet.")
else:
    print(f"\nFant {len(fcs)} feature class(es):\n")

    for fc in fcs:
        print("--------------------------------------------------")
        print(f"Lagnavn: {fc}")

        full_path = os.path.join(GPKG_PATH, fc)

        desc = arcpy.Describe(full_path)

        print(f"Geometri-type: {desc.shapeType}")
        print(f"Koordinatsystem: {desc.spatialReference.name}")

        print("\nFelter:")
        fields = arcpy.ListFields(full_path)

        for f in fields:
            print(f"  {f.name:30}  {f.type}")

print("\n=== FERDIG ===")