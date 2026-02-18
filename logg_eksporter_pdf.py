import csv
import dialogs
from utils import load_logs, current_month


month = current_month()
logs = load_logs()

if not logs:
    print("Ingen data å eksportere")
    raise SystemExit


# ---------- CSV ----------
csv_name = f'Målebil_logg_{month}.csv'
with open(csv_name, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow([
        'Dato', 'Strekning', 'Stopp-type',
        'Tid (min)', 'Vest', 'Blink', 'Avvik'
    ])

    for l in logs:
        writer.writerow([
            l['dato'],
            l['strekning'],
            l['stopp_type'],
            l['varighet_min'],
            'Ja' if l['vest'] else 'Nei',
            'Ja' if l['blink'] else 'Nei',
            l.get('avvik', '')
        ])


# ---------- HTML ----------
total_min = sum(l['varighet_min'] for l in logs)

html_name = f'Målebil_logg_{month}.html'
with open(html_name, 'w', encoding='utf-8') as f:
    f.write(f"""<!DOCTYPE html>
<html lang="no">
<head>
<meta charset="UTF-8">
<title>Målebil logg {month}</title>
<style>
body {{
    font-family: -apple-system, Helvetica, Arial;
    margin: 20px;
}}
table {{
    border-collapse: collapse;
    width: 100%;
}}
th, td {{
    border: 1px solid #333;
    padding: 6px;
    text-align: center;
}}
th {{
    background: #eee;
}}
.summary {{
    margin-bottom: 20px;
    font-weight: bold;
}}
</style>
</head>
<body>

<h1>Målebil logg – {month}</h1>

<div class="summary">
Totalt stopp: {total_min} minutter ({total_min/60:.1f} timer)
</div>

<table>
<tr>
<th>Dato</th>
<th>Strekning</th>
<th>Stopp-type</th>
<th>Tid (min)</th>
<th>Vest</th>
<th>Blink</th>
<th>Avvik</th>
</tr>
""")

    for l in logs:
        f.write(f"""
<tr>
<td>{l['dato']}</td>
<td>{l['strekning']}</td>
<td>{l['stopp_type']}</td>
<td>{l['varighet_min']}</td>
<td>{'Ja' if l['vest'] else 'Nei'}</td>
<td>{'Ja' if l['blink'] else 'Nei'}</td>
<td>{l.get('avvik', '')}</td>
</tr>
""")

    f.write("""
</table>
</body>
</html>
""")


# ---------- iOS Share Sheet ----------
dialogs.share_file(html_name)
dialogs.share_file(csv_name)

print("HTML- og CSV-rapport klar ✅")