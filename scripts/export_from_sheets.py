#!/usr/bin/env python3
"""
Bootstrap local CSV files from existing Google Sheets data.

Uses the credentials and sheet config from src/main/resources/application.yml —
no extra configuration needed.

Requirements:
    pip install google-auth google-api-python-client pyyaml

Usage:
    python3 scripts/export_from_sheets.py
"""

import csv
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    import yaml
except ImportError:
    sys.exit("Missing dependency: pip install pyyaml")

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
except ImportError:
    sys.exit("Missing dependency: pip install google-auth google-api-python-client")

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "src/main/resources/application.yml"

FULL_HEADER = [
    "ingested_at_utc", "event_time_utc", "event_name", "event_type", "event_id",
    "event_trigger_source", "event_classification", "global_id", "device_id",
    "rfid_code", "cat_label",
]
LLM_HEADER = ["event_time_utc", "event_time_local", "direction", "event_classification", "cat_label"]

DIRECTION_MAP = {
    "Exit Allowed": "out",
    "Entry Allowed": "in",
    "Remote": "remote",
    "Manual": "manual",
}


def load_config():
    if not CONFIG_PATH.exists():
        sys.exit(f"Config not found at {CONFIG_PATH}")
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def fetch_sheet_rows(credentials_path, spreadsheet_id, sheet_name):
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}")
        .execute()
    )
    return result.get("values", [])


def to_local_time(utc_str, tz):
    if not utc_str:
        return ""
    try:
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return ""


def pad(row, length):
    return row + [""] * max(0, length - len(row))


def main():
    config = load_config()

    sheets_cfg = config.get("sheets", {})
    credentials_path = sheets_cfg.get("credentialsPath")
    spreadsheet_id = sheets_cfg.get("spreadsheetId")
    sheet_name = sheets_cfg.get("sheetName")

    if not all([credentials_path, spreadsheet_id, sheet_name]):
        sys.exit("sheets.credentialsPath, spreadsheetId, and sheetName must all be set in application.yml")

    file_cfg = config.get("output", {}).get("file", {})
    full_path = PROJECT_ROOT / file_cfg.get("path", "onlycat-events.csv")
    llm_path = PROJECT_ROOT / file_cfg.get("llmPath", "onlycat-events-llm.csv")
    timezone_str = file_cfg.get("timezone", "Europe/London")

    try:
        tz = ZoneInfo(timezone_str)
    except ZoneInfoNotFoundError:
        sys.exit(f"Unknown timezone: {timezone_str}")

    print(f"Fetching from sheet '{sheet_name}' ({spreadsheet_id})...")
    rows = fetch_sheet_rows(credentials_path, spreadsheet_id, sheet_name)

    if not rows:
        print("Sheet is empty, nothing to export.")
        return

    # Use the sheet's own header row to find column positions
    sheet_header = rows[0]
    def col(name):
        try:
            return sheet_header.index(name)
        except ValueError:
            return None

    col_event_time_utc = col("event_time_utc")
    col_trigger_source = col("event_trigger_source")
    col_classification = col("event_classification")
    col_cat_label = col("cat_label")

    data_rows = rows[1:]
    print(f"Exporting {len(data_rows)} rows...")

    with open(full_path, "w", newline="") as full_f, open(llm_path, "w", newline="") as llm_f:
        full_writer = csv.writer(full_f)
        llm_writer = csv.writer(llm_f)

        full_writer.writerow(FULL_HEADER)
        llm_writer.writerow(LLM_HEADER)

        for raw_row in data_rows:
            row = pad(raw_row, len(FULL_HEADER))
            full_writer.writerow(row)

            event_time_utc = row[col_event_time_utc] if col_event_time_utc is not None else ""
            trigger_source = row[col_trigger_source] if col_trigger_source is not None else ""
            classification = row[col_classification] if col_classification is not None else ""
            cat_label = row[col_cat_label] if col_cat_label is not None else ""

            llm_writer.writerow([
                event_time_utc,
                to_local_time(event_time_utc, tz),
                DIRECTION_MAP.get(trigger_source, "unknown"),
                classification,
                cat_label,
            ])

    print(f"Full records written to: {full_path}")
    print(f"LLM-optimised written to: {llm_path}")


if __name__ == "__main__":
    main()
