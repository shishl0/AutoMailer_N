import os
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import smtplib
from jinja2 import Environment, FileSystemLoader
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ---------------------- Load .env secrets ----------------------
load_dotenv()
SMTP_HOST = os.getenv('SMTP_HOST')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASS = os.getenv('SMTP_PASS')

# ---------------------- Paths & Config --------------------------
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / 'config.json'

# Load config.json into `cfg` dict
def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

cfg = load_config()

# Dynamic variables from config
EXCEL_PATH     = BASE_DIR / cfg['excel_path']
HEADER_ROW     = cfg.get('header_row', 1)
COLUMNS        = cfg['columns']      # dict with keys 'fio','email','result'
NEEDED_COLUMNS = cfg['needed_columns']
SUBJECT        = cfg['subject']
TEMPLATES_DIR  = BASE_DIR / cfg.get('templates_dir', 'templates')
LOG_FILE       = cfg.get('log_file', 'mailer.log')

# ---------------------- Logging Setup --------------------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# ---------------------- Jinja2 Setup ---------------------------
jinja_env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=True
)

# ---------------------- Helper Functions -----------------------
def save_config(updated_cfg):
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(updated_cfg, f, ensure_ascii=False, indent=2)


def send_email(to_addr: str, subject: str, body: str):
    msg = f"From: {SMTP_USER}\r\nTo: {to_addr}\r\nSubject: {subject}\r\n\r\n{body}"
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, to_addr, msg)
    logging.info(f"Sent to {to_addr}: {subject}")

# ---------------------- Core Processing ------------------------
def process_sheet():
    # Load or init config state
    state = load_config() if CONFIG_PATH.exists() else {**cfg, 'last_processed_row': 1}
    last_row = state.get('last_processed_row', 1)

    # Read full sheet
    df_full = pd.read_excel(
        EXCEL_PATH,
        engine='openpyxl',
        header=HEADER_ROW
    )
    # Clean column names
    df_full.columns = df_full.columns.str.strip()
    # Filter only needed columns
    df = df_full.filter(items=NEEDED_COLUMNS)

    total_rows = len(df)
    for idx in range(last_row, total_rows):
        row = df.iloc[idx]
        fio    = row[COLUMNS['fio']]
        email  = row[COLUMNS['email']]
        result = row[COLUMNS['result']]

        # Choose template based on result
        template_name = (
            'passed.j2' if str(result).strip().lower() == 'прошел'
            else 'failed.j2'
        )
        template = jinja_env.get_template(template_name)
        body = template.render(fio=fio)

        try:
            send_email(email, SUBJECT, body)
        except Exception as e:
            logging.error(f"Error sending to {email}: {e}")
        else:
            # Update state
            state['last_processed_row'] = idx + 1
            save_config(state)

# ---------------------- File Watcher ---------------------------
class ExcelChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith(EXCEL_PATH.name):
            logging.info("Detected change in Excel, starting processing...")
            process_sheet()

# ---------------------- Main Entry -----------------------------
if __name__ == '__main__':
    if not EXCEL_PATH.exists():
        logging.error(f"Excel file not found: {EXCEL_PATH}")
        exit(1)

    # Initial run
    process_sheet()

    # Start watcher
    observer = Observer()
    observer.schedule(ExcelChangeHandler(), path=str(EXCEL_PATH.parent), recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()