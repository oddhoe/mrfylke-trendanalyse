import ui
import json
import os
from datetime import datetime


def current_month_filename():
    month = datetime.now().strftime('%Y-%m')
    return f'logg_{month}.json'


def lagre_logg(data, filename):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            logs = json.load(f)
    else:
        logs = []

    logs.append(data)

    with open(filename, 'w') as f:
        json.dump(logs, f, indent=2, ensure_ascii=False)

    print(f"Lagt til! Total: {len(logs)} poster")


def lagre_post():
    try:
        varighet_sek = int(varighet.text)
    except (ValueError, TypeError):
        varighet_sek = 0

    data = {
        'dato': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'strekning': strekning.text.strip(),
        'stopp_type': stopp_type.text.strip(),
        'varighet_sek': varighet_sek,
        'vest': True,
        'blink': True,
        'avvik': ''
    }

    lagre_logg(data, current_month_filename())
    v.close()


# ---------- GUI ----------
v = ui.View(frame=(0, 0, 400, 500))
v.background_color = 'white'

tittel = ui.Label(
    text='Ny loggpost',
    frame=(20, 20, 360, 40),
    font=('Helvetica-Bold', 20)
)
v.add_subview(tittel)

label1 = ui.Label(text='Fylkesveg nr:', frame=(20, 80, 120, 30))
strekning = ui.TextField(frame=(150, 80, 230, 30))

label2 = ui.Label(text='Stopp-type:', frame=(20, 130, 120, 30))
stopp_type = ui.TextField(frame=(150, 130, 230, 30))

label3 = ui.Label(text='Varighet (sek):', frame=(20, 180, 140, 30))
varighet = ui.TextField(frame=(170, 180, 140, 30), keyboard_type=ui.KEYBOARD_NUMBER_PAD)

for w in [label1, strekning, label2, stopp_type, label3, varighet]:
    v.add_subview(w)

knapp = ui.Button(
    title='LAGRE',
    frame=(20, 250, 360, 50),
    background_color='green',
    tint_color='white',
    font=('Helvetica-Bold', 18)
)
knapp.action = lambda sender: lagre_post()
v.add_subview(knapp)

v.present('sheet')