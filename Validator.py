"""
ETL Script: Hostname Lookup from Resource Inventory
=====================================================
Accepts two Excel files:
  1. Resource Inventory  - master list of all hosts with details
  2. Verification Sheet  - hostnames to verify (one or more sheets)

Matches hostnames, extracts desired columns, and generates a report
Excel + a draft email (HTML file you can open and copy into your mail client).

HOW TO RUN:
    python etl_hostname_lookup.py \
        --inventory  "Resource_Inventory.xlsx" \
        --verification "Verification.xlsx" \
        --output     "Lookup_Results.xlsx"

    All column names and behaviour can be changed in the CONFIG block below.
"""

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  ← Edit everything here; no need to touch the logic below
# ─────────────────────────────────────────────────────────────────────────────
CONFIG = {

    # ── Inventory file ────────────────────────────────────────────────────────
    # Name of the column in the Resource Inventory that holds the hostname.
    "inventory_hostname_col": "Instance Name",

    # Columns to pull from the inventory into the report.
    # Add or remove column names freely.  Order is preserved in the output.
    "inventory_desired_cols": [
        "Instance Name",
        "IP Address",
        "Environment",
        "OS",
        "Owner",
        "Status",
    ],

    # Which sheet inside the inventory file to read (0 = first sheet, or use
    # the sheet name as a string, e.g. "Inventory").
    "inventory_sheet": 0,

    # ── Verification file ─────────────────────────────────────────────────────
    # Name of the column in every verification sheet that holds the hostname.
    "verification_hostname_col": "Name",

    # If True, ALL sheets in the verification file are processed.
    # If False, only the sheets listed in verification_sheet_names are used.
    "verification_all_sheets": True,

    # Used only when verification_all_sheets = False.
    "verification_sheet_names": ["Sheet1", "Sheet2"],

    # ── Matching behaviour ────────────────────────────────────────────────────
    # When a value in the verification sheet contains "@", only the part AFTER
    # the "@" (stripped of whitespace) is used as the hostname to look up.
    # Example: "Server @ myhost.corp" → lookup key = "myhost.corp"
    "strip_before_at": True,

    # Case-insensitive matching (recommended: True).
    "case_insensitive": True,

    # ── Output ────────────────────────────────────────────────────────────────
    # Text written in the "Status" column for records that were not found.
    "not_found_message": "Hostname not found in the recent inventory",

    # Name of the "Status" column added to the output.
    "status_col_name": "Lookup Status",

    # Name of the "Source Sheet" column added to the output (shows which
    # verification sheet a row came from).
    "source_sheet_col_name": "Source Sheet",

    # ── Email draft ───────────────────────────────────────────────────────────
    "email_to": "recipient@example.com",
    "email_cc": "",
    "email_subject": "Hostname Verification Report",
    "email_body": (
        "Hi,\n\n"
        "Please find attached the hostname verification report generated from "
        "the latest resource inventory.\n\n"
        "The report contains:\n"
        "  • Found entries  – full inventory details for matched hostnames.\n"
        "  • Not Found entries – hostnames that could not be located in the "
        "current inventory.\n\n"
        "Kindly review and revert with any corrections.\n\n"
        "Regards"
    ),
}
# ─────────────────────────────────────────────────────────────────────────────
# END OF CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────


import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import (
    Alignment,
    Font,
    PatternFill,
    Border,
    Side,
)
from openpyxl.utils import get_column_letter


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_hostname(raw: str, strip_at: bool) -> str:
    """Extract the lookup key from a raw cell value."""
    raw = str(raw).strip()
    if strip_at and "@" in raw:
        raw = raw.split("@", 1)[1].strip()
    return raw


def normalise(value: str, case_insensitive: bool) -> str:
    return str(value).strip().lower() if case_insensitive else str(value).strip()


def load_inventory(path: str, cfg: dict) -> pd.DataFrame:
    """Load the inventory sheet and validate required columns."""
    inv = pd.read_excel(
        path,
        sheet_name=cfg["inventory_sheet"],
        dtype=str,
    ).fillna("")

    missing = [
        c for c in [cfg["inventory_hostname_col"]] + cfg["inventory_desired_cols"]
        if c not in inv.columns
    ]
    if missing:
        print(
            f"\n[ERROR] The following columns were NOT found in the inventory:\n"
            f"        {missing}\n"
            f"        Available columns: {list(inv.columns)}\n"
        )
        sys.exit(1)

    return inv


def build_lookup(inv: pd.DataFrame, cfg: dict) -> dict:
    """Build a dict keyed by normalised hostname → list of inventory rows."""
    lookup: dict = {}
    key_col = cfg["inventory_hostname_col"]
    ci = cfg["case_insensitive"]
    for _, row in inv.iterrows():
        key = normalise(row[key_col], ci)
        lookup.setdefault(key, []).append(row)
    return lookup


def process_verification(vpath: str, cfg: dict, lookup: dict) -> list[dict]:
    """
    Iterate every requested sheet in the verification file.
    Returns a flat list of result dicts.
    """
    if cfg["verification_all_sheets"]:
        all_sheets = pd.read_excel(vpath, sheet_name=None, dtype=str)
        sheets_to_process = list(all_sheets.items())
    else:
        sheets_to_process = [
            (name, pd.read_excel(vpath, sheet_name=name, dtype=str))
            for name in cfg["verification_sheet_names"]
        ]

    results = []
    hn_col = cfg["verification_hostname_col"]
    ci = cfg["case_insensitive"]
    desired = cfg["inventory_desired_cols"]
    not_found_msg = cfg["not_found_message"]

    for sheet_name, df in sheets_to_process:
        df = df.fillna("")
        if hn_col not in df.columns:
            print(
                f"[WARNING] Sheet '{sheet_name}' does not have a column "
                f"'{hn_col}'. Skipping."
            )
            continue

        for _, vrow in df.iterrows():
            raw_val = str(vrow[hn_col]).strip()
            if not raw_val:
                continue  # skip blank rows

            parsed_host = parse_hostname(raw_val, cfg["strip_before_at"])
            key = normalise(parsed_host, ci)

            if key in lookup:
                for inv_row in lookup[key]:
                    record = {cfg["source_sheet_col_name"]: sheet_name}
                    record["Original Verification Value"] = raw_val
                    for col in desired:
                        record[col] = inv_row.get(col, "")
                    record[cfg["status_col_name"]] = "Found"
                    results.append(record)
            else:
                record = {cfg["source_sheet_col_name"]: sheet_name}
                record["Original Verification Value"] = raw_val
                for col in desired:
                    record[col] = ""
                record[cfg["status_col_name"]] = not_found_msg
                results.append(record)

    return results


# ── Excel output ──────────────────────────────────────────────────────────────

# Colour palette
HEADER_FILL   = PatternFill("solid", fgColor="1F3864")   # dark navy
FOUND_FILL    = PatternFill("solid", fgColor="E8F5E9")   # very light green
NOTFOUND_FILL = PatternFill("solid", fgColor="FFEBEE")   # very light red
ALT_ROW_FILL  = PatternFill("solid", fgColor="F5F5F5")   # light grey stripe

HEADER_FONT  = Font(name="Arial", bold=True, color="FFFFFF", size=11)
CELL_FONT    = Font(name="Arial", size=10)

THIN_SIDE    = Side(style="thin", color="BDBDBD")
THIN_BORDER  = Border(left=THIN_SIDE, right=THIN_SIDE,
                      top=THIN_SIDE,  bottom=THIN_SIDE)


def write_output_excel(results: list[dict], output_path: str, cfg: dict):
    df = pd.DataFrame(results)
    if df.empty:
        print("[WARNING] No results to write.")
        return

    # Split: found vs not-found
    found_mask = df[cfg["status_col_name"]] == "Found"

    wb = load_workbook(pd.ExcelWriter.__new__(pd.ExcelWriter)) if False else \
         _create_workbook_from_df(df, cfg, found_mask)

    wb.save(output_path)
    print(f"[OK] Results saved → {output_path}")


def _style_sheet(ws, df_sheet: pd.DataFrame, found_mask, cfg):
    """Apply formatting to a worksheet populated from df_sheet."""
    for col_idx, col_name in enumerate(df_sheet.columns, start=1):
        cell = ws.cell(row=1, column=col_idx)
        cell.value = col_name
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center",
                                   wrap_text=True)
        cell.border = THIN_BORDER

    status_col_idx = list(df_sheet.columns).index(cfg["status_col_name"]) + 1

    for row_idx, (df_idx, row) in enumerate(df_sheet.iterrows(), start=2):
        is_found = found_mask.loc[df_idx] if df_idx in found_mask.index else True
        row_fill = FOUND_FILL if is_found else NOTFOUND_FILL
        if is_found and row_idx % 2 == 0:
            row_fill = ALT_ROW_FILL  # subtle striping for found rows

        for col_idx, value in enumerate(row, start=1):
            cell = ws.cell(row=row_idx, column=col_idx)
            cell.value = value
            cell.font = CELL_FONT
            cell.border = THIN_BORDER
            cell.alignment = Alignment(vertical="center", wrap_text=False)
            if col_idx == status_col_idx:
                cell.fill = FOUND_FILL if is_found else NOTFOUND_FILL
            else:
                cell.fill = row_fill

    # Auto-width
    for col_idx, col_name in enumerate(df_sheet.columns, start=1):
        col_letter = get_column_letter(col_idx)
        max_len = max(
            len(str(col_name)),
            *(len(str(ws.cell(r, col_idx).value or ""))
              for r in range(2, ws.max_row + 1))
        )
        ws.column_dimensions[col_letter].width = min(max_len + 4, 50)

    ws.row_dimensions[1].height = 30
    ws.freeze_panes = "A2"


def _create_workbook_from_df(df: pd.DataFrame, cfg: dict, found_mask):
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)  # remove default blank sheet

    # Sheet 1: All Results
    ws_all = wb.create_sheet("All Results")
    _style_sheet(ws_all, df, found_mask, cfg)

    # Sheet 2: Found Only
    df_found = df[found_mask].reset_index(drop=False)
    if not df_found.empty:
        ws_found = wb.create_sheet("Found")
        _style_sheet(ws_found, df_found.drop(columns="index"), found_mask.reindex(df_found["index"]).fillna(True), cfg)

    # Sheet 3: Not Found Only
    df_nf = df[~found_mask].reset_index(drop=False)
    if not df_nf.empty:
        ws_nf = wb.create_sheet("Not Found")
        all_false = pd.Series(False, index=df_nf["index"])
        _style_sheet(ws_nf, df_nf.drop(columns="index"), all_false, cfg)

    # Sheet 4: Summary
    ws_sum = wb.create_sheet("Summary")
    _write_summary(ws_sum, df, cfg)

    return wb


def _write_summary(ws, df: pd.DataFrame, cfg: dict):
    total      = len(df)
    found_cnt  = (df[cfg["status_col_name"]] == "Found").sum()
    nf_cnt     = total - found_cnt
    pct_found  = f"{found_cnt/total*100:.1f}%" if total else "N/A"

    ws.title = "Summary"
    ws["A1"] = "Hostname Verification — Summary"
    ws["A1"].font = Font(name="Arial", bold=True, size=14, color="1F3864")
    ws["A2"] = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    ws["A2"].font = Font(name="Arial", size=10, color="757575")

    rows = [
        ("Metric", "Value"),
        ("Total Hostnames Processed", total),
        ("Found in Inventory", found_cnt),
        ("Not Found in Inventory", nf_cnt),
        ("Match Rate", pct_found),
    ]

    sheets_processed = df[cfg["source_sheet_col_name"]].unique()
    rows.append(("", ""))
    rows.append(("Verification Sheets Processed", ""))
    for s in sheets_processed:
        sheet_df = df[df[cfg["source_sheet_col_name"]] == s]
        sf = (sheet_df[cfg["status_col_name"]] == "Found").sum()
        rows.append((f"  {s}", f"{sf}/{len(sheet_df)} found"))

    for r_idx, (label, value) in enumerate(rows, start=4):
        c_label = ws.cell(row=r_idx, column=1, value=label)
        c_value = ws.cell(row=r_idx, column=2, value=value)
        if label == "Metric":
            for c in (c_label, c_value):
                c.font   = HEADER_FONT
                c.fill   = HEADER_FILL
                c.border = THIN_BORDER
                c.alignment = Alignment(horizontal="center")
        else:
            for c in (c_label, c_value):
                c.font   = CELL_FONT
                c.border = THIN_BORDER
            if label:
                c_label.font = Font(name="Arial", size=10, bold=True)

    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 20


# ── Email draft ───────────────────────────────────────────────────────────────

def generate_email_draft(output_excel: str, results: list[dict], cfg: dict,
                          draft_path: str):
    found  = sum(1 for r in results if r[cfg["status_col_name"]] == "Found")
    nf     = len(results) - found
    body   = cfg["email_body"].replace("\n", "<br>")
    attach = os.path.basename(output_excel)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body      {{ font-family: Arial, sans-serif; margin: 30px; color: #212121; }}
  .card     {{ border: 1px solid #e0e0e0; border-radius: 8px; padding: 24px;
               max-width: 680px; background: #fff; }}
  .field    {{ margin-bottom: 10px; }}
  .label    {{ font-size: 12px; font-weight: bold; color: #757575;
               text-transform: uppercase; letter-spacing: .5px; }}
  .value    {{ font-size: 14px; color: #1a1a1a; padding: 6px 0; border-bottom: 1px solid #f0f0f0; }}
  .body-box {{ background: #f9f9f9; border-left: 4px solid #1F3864;
               padding: 14px 18px; border-radius: 4px; margin: 16px 0;
               font-size: 14px; line-height: 1.7; white-space: pre-wrap; }}
  .badge    {{ display: inline-block; padding: 3px 10px; border-radius: 12px;
               font-size: 12px; font-weight: bold; margin: 2px; }}
  .green    {{ background: #e8f5e9; color: #2e7d32; }}
  .red      {{ background: #ffebee; color: #c62828; }}
  .attach   {{ font-size: 13px; background: #e3f2fd; border: 1px dashed #90caf9;
               padding: 8px 14px; border-radius: 6px; display: inline-block; }}
  h2        {{ color: #1F3864; margin-bottom: 4px; }}
  .note     {{ font-size: 11px; color: #9e9e9e; margin-top: 18px; }}
</style>
</head>
<body>
<div class="card">
  <h2>📧 Email Draft</h2>
  <p style="font-size:12px;color:#9e9e9e;margin-top:0">
    Copy the fields below into your email client.
  </p>

  <div class="field">
    <div class="label">To</div>
    <div class="value">{cfg["email_to"]}</div>
  </div>

  {"<div class='field'><div class='label'>CC</div><div class='value'>" + cfg["email_cc"] + "</div></div>" if cfg["email_cc"] else ""}

  <div class="field">
    <div class="label">Subject</div>
    <div class="value">{cfg["email_subject"]}</div>
  </div>

  <div class="field">
    <div class="label">Body</div>
    <div class="body-box">{body}</div>
  </div>

  <div class="field">
    <div class="label">Attachment</div><br>
    <span class="attach">📎 {attach}</span>
  </div>

  <div style="margin-top:18px">
    <span class="badge green">✔ Found: {found}</span>
    <span class="badge red">✘ Not Found: {nf}</span>
    <span class="badge" style="background:#e8eaf6;color:#283593">
      Total: {found + nf}
    </span>
  </div>

  <p class="note">
    Generated by ETL Hostname Lookup Script · {datetime.now().strftime('%Y-%m-%d %H:%M')}
  </p>
</div>
</body>
</html>"""

    with open(draft_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Email draft saved  → {draft_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="ETL: Look up verification hostnames in a resource inventory."
    )
    p.add_argument("--inventory",     required=True,
                   help="Path to the Resource Inventory Excel file.")
    p.add_argument("--verification",  required=True,
                   help="Path to the Verification Excel file.")
    p.add_argument("--output",        default="Lookup_Results.xlsx",
                   help="Output Excel file path (default: Lookup_Results.xlsx).")
    p.add_argument("--email-draft",   default="",
                   help="Path for the HTML email draft (default: auto-named).")
    return p.parse_args()


def main():
    args = parse_args()

    # Validate inputs
    for fpath, label in [(args.inventory, "Inventory"), (args.verification, "Verification")]:
        if not Path(fpath).is_file():
            print(f"[ERROR] {label} file not found: {fpath}")
            sys.exit(1)

    cfg = CONFIG

    print("\n──────────────────────────────────────────")
    print("  ETL Hostname Lookup")
    print("──────────────────────────────────────────")
    print(f"  Inventory    : {args.inventory}")
    print(f"  Verification : {args.verification}")
    print(f"  Output       : {args.output}")
    print("──────────────────────────────────────────\n")

    print("[1/4] Loading resource inventory …")
    inv = load_inventory(args.inventory, cfg)
    print(f"      {len(inv)} rows loaded.")

    print("[2/4] Building lookup index …")
    lookup = build_lookup(inv, cfg)
    print(f"      {len(lookup)} unique hostnames indexed.")

    print("[3/4] Processing verification sheets …")
    results = process_verification(args.verification, cfg, lookup)
    found = sum(1 for r in results if r[cfg["status_col_name"]] == "Found")
    nf    = len(results) - found
    print(f"      {len(results)} rows processed → {found} found, {nf} not found.")

    print("[4/4] Writing output files …")
    write_output_excel(results, args.output, cfg)

    draft_path = args.email_draft or args.output.replace(".xlsx", "_email_draft.html")
    generate_email_draft(args.output, results, cfg, draft_path)

    print("\n✅ Done!\n")


if __name__ == "__main__":
    main()
