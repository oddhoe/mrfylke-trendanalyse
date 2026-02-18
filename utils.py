import json
import os
from datetime import datetime


def current_month():
    return datetime.now().strftime('%Y-%m')


def log_filename(month=None):
    m = month or current_month()
    return f'logg_{m}.json'


def load_logs(month=None):
    filename = log_filename(month)
    if not os.path.exists(filename):
        return []
    with open(filename, 'r') as f:
        return json.load(f)


def save_logs(logs, month=None):
    filename = log_filename(month)
    with open(filename, 'w') as f:
        json.dump(logs, f, indent=2, ensure_ascii=False)


def archive_old_month():
    this_month = current_month()
    for file in os.listdir('.'):
        if file.startswith('logg_') and file.endswith('.json') and this_month not in file:
            os.makedirs('arkiv', exist_ok=True)
            os.rename(file, os.path.join('arkiv', file))