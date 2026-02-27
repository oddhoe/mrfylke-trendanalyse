import arcpy
fc = r"D:\Conda\Flaskehasler_git\mrfylke-trendanalyse\Normaltransport\gdb\nvdb_radata.gdb\Bruksklasse_904"
for f in arcpy.ListFields(fc):
    print(f.name, "-", f.type)
