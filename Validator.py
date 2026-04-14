import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


# =============================================================================
#  CONFIGURATION  <-- All user-editable settings live here
# =============================================================================
CONFIG = {
    # File paths
    "inventory_file": "Resource_Inventory.xlsx",
    "verification_file": "Verification.xlsx",
    "output_file": "Lookup_Results.xlsx",

    # Inventory columns
    "inventory_hostname_col": "Instance Name",
    "inventory_desired_cols": [
        "Instance Name",
        "IP Address",
        "Environment",
        "OS",
        "Owner",
        "Status",
    ],
    # Use 0 for first sheet or sheet name string
    "inventory_sheet": 0,

    # Verification columns
    "verification_hostname_col": "Name",
    "verification_all_sheets": True,
    "verification_sheet_names": ["Sheet1", "Sheet2"],

    # Matching behavior
    "strip_before_at": True,
    "case_insensitive": True,

    # Output labels
    "not_found_message": "Hostname not found in the recent inventory",
    "status_col_name": "Lookup Status",
    "source_sheet_col_name": "Source Sheet",
    "original_value_col_name": "Original Verification Value",

    # Outlook draft email fields
    "email_to": "recipient@example.com",
    "email_cc": "",
    "email_subject": "Hostname Verification Report",
    "email_body": (
        "Hi,\n\n"
        "Please find attached the hostname verification report generated from "
        "the latest resource inventory.\n\n"
        "The report contains:\n"
        "  * One tab per verification sheet (e.g., AWS, Azure).\n"
        "  * Each tab shows requested hostnames with matched inventory details.\n"
        "  * Rows not found in inventory are clearly marked.\n\n"
        "Kindly review and revert with any corrections.\n\n"
        "Regards"
    ),

    # If True, attach output report to Outlook draft
    "attach_report_to_email": True,

    # If True, keep a Summary worksheet in Excel output
    "include_summary_sheet": True,
}
# =============================================================================
#  END OF CONFIGURATION -- do not edit below this line
# =============================================================================


# Style constants
_HEADER_FILL = PatternFill("solid", fgColor="1F3864")
_FOUND_FILL = PatternFill("solid", fgColor="E8F5E9")
_NOTFOUND_FILL = PatternFill("solid", fgColor="FFEBEE")
_ALT_ROW_FILL = PatternFill("solid", fgColor="F5F5F5")

_HEADER_FONT = Font(name="Arial", bold=True, color="FFFFFF", size=11)
_CELL_FONT = Font(name="Arial", size=10)
_BOLD_FONT = Font(name="Arial", bold=True, size=10)

_THIN = Side(style="thin", color="BDBDBD")
_BORDER = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)


# =============================================================================
#  Helpers
# =============================================================================

def _parse_hostname(raw: str, strip_at: bool) -> str:
    """Return the lookup key extracted from a raw cell value."""
    raw = str(raw).strip()
    if strip_at and "@" in raw:
        raw = raw.split("@", 1)[1].strip()
    return raw


def _normalise(value: str, case_insensitive: bool) -> str:
    s = str(value).strip()
    return s.lower() if case_insensitive else s


def _read_tabular(path: str, sheet_name=0):
    """
    Read either Excel or CSV safely.

    Supported:
      - Excel: .xlsx .xls .xlsm .xlsb .ods
      - CSV:   .csv

    Behavior:
      - If sheet_name is None:
          * Excel -> dict[sheet_name, DataFrame]
          * CSV   -> {"Sheet1": DataFrame}
      - Else:
          * returns single DataFrame
    """
    p = Path(path)
    ext = p.suffix.lower()

    if ext == ".csv":
        df = pd.read_csv(path, dtype=str).fillna("")
        df.columns = [str(c).strip() for c in df.columns]
        if sheet_name is None:
            return {"Sheet1": df}
        return df

    excel_exts = {".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"}
    if ext in excel_exts:
        data = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
        if isinstance(data, dict):
            out = {}
            for k, v in data.items():
                v = v.fillna("")
                v.columns = [str(c).strip() for c in v.columns]
                out[k] = v
            return out
        data = data.fillna("")
        data.columns = [str(c).strip() for c in data.columns]
        return data

    # Fallback attempt
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
        df.columns = [str(c).strip() for c in df.columns]
        if sheet_name is None:
            return {"Sheet1": df}
        return df
    except Exception:
        data = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
        if isinstance(data, dict):
            out = {}
            for k, v in data.items():
                v = v.fillna("")
                v.columns = [str(c).strip() for c in v.columns]
                out[k] = v
            return out
        data = data.fillna("")
        data.columns = [str(c).strip() for c in data.columns]
        return data


# =============================================================================
#  ETL steps
# =============================================================================

def load_inventory(path: str, cfg: dict) -> pd.DataFrame:
    """Load the inventory sheet/file and validate required columns."""
    inv = _read_tabular(path, sheet_name=cfg["inventory_sheet"])

    if isinstance(inv, dict):
        sheet_key = cfg["inventory_sheet"]
        if isinstance(sheet_key, str) and sheet_key in inv:
            inv = inv[sheet_key]
        elif isinstance(sheet_key, int):
            keys = list(inv.keys())
            inv = inv[keys[sheet_key]] if 0 <= sheet_key < len(keys) else inv[keys[0]]
        else:
            inv = next(iter(inv.values()))

    if not isinstance(inv, pd.DataFrame):
        print(f"[ERROR] Inventory data invalid type: {type(inv)}")
        sys.exit(1)

    inv = inv.fillna("")
    inv.columns = [str(c).strip() for c in inv.columns]

    required = list(dict.fromkeys([cfg["inventory_hostname_col"]] + cfg["inventory_desired_cols"]))
    missing = [c for c in required if c not in inv.columns]
    if missing:
        print(
            "\n[ERROR] Columns not found in the inventory:"
            f"\n        {missing}"
            f"\n        Available columns: {list(inv.columns)}\n"
        )
        sys.exit(1)

    return inv


def build_lookup(inv: pd.DataFrame, cfg: dict) -> dict:
    """Build dict: normalised hostname -> list of matching inventory rows."""
    lookup = {}
    key_col = cfg["inventory_hostname_col"]
    ci = cfg["case_insensitive"]

    for _, row in inv.iterrows():
        key = _normalise(row.get(key_col, ""), ci)
        if key:
            lookup.setdefault(key, []).append(row)

    dup_hosts = [k for k, v in lookup.items() if len(v) > 1]
    if dup_hosts:
        print(f"[WARNING] {len(dup_hosts)} duplicate hostname(s) in inventory; all matches will be returned.")

    return lookup


def process_verification(vpath: str, cfg: dict, lookup: dict) -> tuple[list, dict]:
    """
    Returns:
      all_results: flat list of all output records
      per_sheet_results: dict[sheet_name] = list of output records for that sheet
    """
    if cfg["verification_all_sheets"]:
        raw_sheets = _read_tabular(vpath, sheet_name=None)
        sheets = list(raw_sheets.items()) if isinstance(raw_sheets, dict) else [("Sheet1", raw_sheets)]
    else:
        sheets = [(name, _read_tabular(vpath, sheet_name=name)) for name in cfg["verification_sheet_names"]]

    hn_col = cfg["verification_hostname_col"]
    ci = cfg["case_insensitive"]
    desired = cfg["inventory_desired_cols"]
    not_found_msg = cfg["not_found_message"]
    src_col = cfg["source_sheet_col_name"]
    orig_col = cfg["original_value_col_name"]
    status_col = cfg["status_col_name"]

    all_results = []
    per_sheet_results = {}

    for sheet_name, df in sheets:
        if isinstance(df, dict):
            df = next(iter(df.values()))
        df = df.fillna("")
        df.columns = [str(c).strip() for c in df.columns]

        if hn_col not in df.columns:
            print(
                f"[WARNING] Sheet '{sheet_name}' has no column '{hn_col}'. "
                f"Available: {list(df.columns)}. Skipping."
            )
            continue

        sheet_rows = []

        for _, vrow in df.iterrows():
            raw_val = str(vrow[hn_col]).strip()
            if not raw_val:
                continue

            parsed = _parse_hostname(raw_val, cfg["strip_before_at"]).strip()
            key = _normalise(parsed, ci)

            if key and key in lookup:
                for inv_row in lookup[key]:
                    record = {src_col: sheet_name, orig_col: raw_val}
                    for col in desired:
                        record[col] = inv_row.get(col, "")
                    record[status_col] = "Found"
                    sheet_rows.append(record)
                    all_results.append(record)
            else:
                record = {src_col: sheet_name, orig_col: raw_val}
                for col in desired:
                    record[col] = ""
                record[status_col] = not_found_msg
                sheet_rows.append(record)
                all_results.append(record)

        per_sheet_results[sheet_name] = sheet_rows

    return all_results, per_sheet_results


# =============================================================================
#  Excel output
# =============================================================================

def _style_sheet(ws, df: pd.DataFrame, found_flags: list, cfg: dict) -> None:
    columns = list(df.columns)
    status_col_idx = columns.index(cfg["status_col_name"]) + 1

    # Header
    for col_idx, col_name in enumerate(columns, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.font = _HEADER_FONT
        cell.fill = _HEADER_FILL
        cell.border = _BORDER
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # Data rows
    for row_pos, (row_values, is_found) in enumerate(
        zip(df.itertuples(index=False, name=None), found_flags), start=2
    ):
        base_fill = (_ALT_ROW_FILL if row_pos % 2 == 0 else _FOUND_FILL) if is_found else _NOTFOUND_FILL

        for col_idx, value in enumerate(row_values, start=1):
            cell = ws.cell(row=row_pos, column=col_idx, value=value)
            cell.font = _BOLD_FONT if col_idx == status_col_idx else _CELL_FONT
            cell.border = _BORDER
            cell.alignment = Alignment(vertical="center", wrap_text=False)
            cell.fill = (_FOUND_FILL if is_found else _NOTFOUND_FILL) if col_idx == status_col_idx else base_fill

    # Auto width
    for col_idx, col_name in enumerate(columns, start=1):
        col_letter = get_column_letter(col_idx)
        lengths = [len(str(ws.cell(r, col_idx).value or "")) for r in range(2, ws.max_row + 1)]
        max_len = max([len(str(col_name))] + lengths if lengths else [len(str(col_name))])
        ws.column_dimensions[col_letter].width = min(max_len + 4, 52)

    ws.row_dimensions[1].height = 30
    ws.freeze_panes = "A2"


def _write_summary(ws, df: pd.DataFrame, cfg: dict) -> None:
    status_col = cfg["status_col_name"]
    total = len(df)
    found_cnt = int((df[status_col] == "Found").sum())
    nf_cnt = total - found_cnt
    pct_found = f"{found_cnt / total * 100:.1f}%" if total else "N/A"

    ws["A1"] = "Hostname Verification -- Summary"
    ws["A1"].font = Font(name="Arial", bold=True, size=14, color="1F3864")
    ws["A2"] = f"Generated: {datetime.now().strftime('%Y-%m-%d  %H:%M')}"
    ws["A2"].font = Font(name="Arial", size=10, color="757575")

    summary_rows = [
        ("Metric", "Value"),
        ("Total Hostnames Processed", total),
        ("Found in Inventory", found_cnt),
        ("Not Found in Inventory", nf_cnt),
        ("Match Rate", pct_found),
        ("", ""),
        ("Breakdown by Verification Sheet", ""),
    ]

    if total > 0:
        for sheet_name in df[cfg["source_sheet_col_name"]].unique():
            sub = df[df[cfg["source_sheet_col_name"]] == sheet_name]
            sf = int((sub[status_col] == "Found").sum())
            summary_rows.append((f"  {sheet_name}", f"{sf} / {len(sub)} found"))

    for r_idx, (label, value) in enumerate(summary_rows, start=4):
        c_label = ws.cell(row=r_idx, column=1, value=label)
        c_value = ws.cell(row=r_idx, column=2, value=value)
        if label == "Metric":
            for c in (c_label, c_value):
                c.font = _HEADER_FONT
                c.fill = _HEADER_FILL
                c.border = _BORDER
                c.alignment = Alignment(horizontal="center", vertical="center")
        elif label:
            c_label.font = _BOLD_FONT
            c_value.font = _CELL_FONT
            for c in (c_label, c_value):
                c.border = _BORDER

    ws.column_dimensions["A"].width = 38
    ws.column_dimensions["B"].width = 22


def _safe_excel_sheet_name(name: str) -> str:
    """Excel sheet names: max 31 chars, cannot contain []:*?/\\ and cannot be empty."""
    s = str(name) if name is not None else "Sheet"
    for ch in ['[', ']', ':', '*', '?', '/', '\\']:
        s = s.replace(ch, "_")
    s = s.strip() or "Sheet"
    return s[:31]


def write_output_excel(results: list, output_path: str, cfg: dict, per_sheet_results: dict) -> None:
    if not results:
        print("[WARNING] No results to write -- output file not created.")
        return

    wb = Workbook()
    wb.remove(wb.active)

    # one worksheet per verification sheet
    for raw_sheet_name, rows in per_sheet_results.items():
        if not rows:
            continue

        safe_name = _safe_excel_sheet_name(raw_sheet_name)

        # avoid duplicate sheet names after sanitization
        final_name = safe_name
        n = 1
        while final_name in wb.sheetnames:
            suffix = f"_{n}"
            final_name = (safe_name[:31 - len(suffix)] + suffix) if len(safe_name) + len(suffix) > 31 else safe_name + suffix
            n += 1

        df_sheet = pd.DataFrame(rows)
        found_flags = [r == "Found" for r in df_sheet[cfg["status_col_name"]]]

        ws = wb.create_sheet(final_name)
        _style_sheet(ws, df_sheet.reset_index(drop=True), found_flags, cfg)

    if cfg.get("include_summary_sheet", True):
        df_all = pd.DataFrame(results)
        ws_sum = wb.create_sheet("Summary")
        _write_summary(ws_sum, df_all, cfg)

    wb.save(output_path)
    print(f"[OK] Report saved         -> {output_path}")


# =============================================================================
#  Outlook draft (pywin32)
# =============================================================================

def create_outlook_draft(output_excel: str, results: list, cfg: dict) -> None:
    """Create Outlook draft email directly (saved in Drafts)."""
    try:
        import win32com.client  # pywin32
    except ImportError:
        print("[ERROR] pywin32 is not installed. Install with: pip install pywin32")
        return

    status_col = cfg["status_col_name"]
    found = sum(1 for r in results if r[status_col] == "Found")
    nf = len(results) - found
    total = len(results)

    body_html = cfg["email_body"].replace("\n", "<br>")
    summary_html = (
        "<br><br><b>Run Summary:</b>"
        f"<br>Found: {found}"
        f"<br>Not Found: {nf}"
        f"<br>Total: {total}"
    )

    html = f"<html><body>{body_html}{summary_html}</body></html>"

    try:
        outlook = win32com.client.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)  # olMailItem

        mail.To = cfg["email_to"]
        mail.CC = cfg["email_cc"]
        mail.Subject = cfg["email_subject"]
        mail.HTMLBody = html

        if cfg.get("attach_report_to_email", True):
            report_path = Path(output_excel).resolve()
            if report_path.is_file():
                mail.Attachments.Add(str(report_path))
            else:
                print(f"[WARNING] Attachment not found: {report_path}")

        mail.Save()  # saves to Drafts
        print("[OK] Outlook draft created -> Drafts folder")
    except Exception as e:
        print(f"[ERROR] Failed to create Outlook draft: {e}")


# =============================================================================
#  CLI
# =============================================================================

def parse_args(cfg: dict) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ETL: Look up verification hostnames in a resource inventory.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--inventory",
        default=cfg["inventory_file"],
        help="Path to the Resource Inventory file (.xlsx/.xls/.csv).",
    )
    p.add_argument(
        "--verification",
        default=cfg["verification_file"],
        help="Path to the Verification file (.xlsx/.xls/.csv).",
    )
    p.add_argument(
        "--output",
        default=cfg["output_file"],
        help="Output Excel report path.",
    )
    return p.parse_args()


def main() -> None:
    cfg = CONFIG
    args = parse_args(cfg)

    inventory_path = args.inventory
    verification_path = args.verification
    output_path = args.output

    errors = []
    for fpath, label in [
        (inventory_path, "Inventory"),
        (verification_path, "Verification"),
    ]:
        if not Path(fpath).is_file():
            errors.append(f"  [{label}] File not found: {fpath}")

    if errors:
        print("\n[ERROR] Cannot start -- input file(s) missing:")
        print("\n".join(errors))
        print("\nSet the correct paths in CONFIG or pass --inventory / --verification.")
        sys.exit(1)

    print()
    print("=" * 52)
    print("  ETL Hostname Lookup")
    print("=" * 52)
    print(f"  Inventory    : {inventory_path}")
    print(f"  Verification : {verification_path}")
    print(f"  Output       : {output_path}")
    print("=" * 52)
    print()

    print("[1/4] Loading resource inventory ...")
    inv = load_inventory(inventory_path, cfg)
    print(f"      {len(inv):,} rows loaded.")

    print("[2/4] Building lookup index ...")
    lookup = build_lookup(inv, cfg)
    print(f"      {len(lookup):,} unique hostnames indexed.")

    print("[3/4] Processing verification sheet(s) ...")
    results, per_sheet_results = process_verification(verification_path, cfg, lookup)
    found = sum(1 for r in results if r[cfg["status_col_name"]] == "Found")
    nf = len(results) - found
    print(f"      {len(results):,} rows -> {found:,} found, {nf:,} not found.")

    print("[4/4] Writing outputs ...")
    write_output_excel(results, output_path, cfg, per_sheet_results)
    create_outlook_draft(output_path, results, cfg)

    print()
    print("Done!")
    print()


if __name__ == "__main__":
    main()
