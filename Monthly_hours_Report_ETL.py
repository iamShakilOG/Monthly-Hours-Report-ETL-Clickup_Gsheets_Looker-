#!/usr/bin/env python3
"""
Monthly Hours Report ETL
- Runs locally or in GitHub Actions
- Reads project + internal log data from Google Sheets
- Fetches industry mapping from ClickUp
- Produces two outputs: Project Report + Merged report
"""

import argparse
import logging
import os
import sys
from typing import Dict, List, Optional

import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pandas.tseries.offsets import MonthEnd


# -----------------------------
# Logging
# -----------------------------

def setup_logging(level: str, log_file: Optional[str]) -> logging.Logger:
    logger = logging.getLogger("monthly_hours_etl")
    logger.setLevel(level)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


# -----------------------------
# Helpers
# -----------------------------

def get_env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    val = os.getenv(name)
    if val is None or val == "":
        val = default
    if required and not val:
        raise SystemExit(f"Missing required env var: {name}")
    return val


def ensure_columns(df: pd.DataFrame, cols: List[str], fill_value="") -> pd.DataFrame:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        for c in missing:
            df[c] = fill_value
    return df


def accuracy_to_ratio(x) -> float:
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return 0.0
    s = s.replace("%", "")
    try:
        v = float(s)
    except Exception:
        return 0.0
    if v > 1:
        v /= 100
    if v < 0:
        v = 0
    if v > 1:
        v = 1
    return round(v, 4)


# -----------------------------
# Google Sheets
# -----------------------------

def build_gspread_client(creds_path: str) -> gspread.Client:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    return gspread.authorize(creds)


def fetch_sheet_df(client: gspread.Client, sheet_key: str, tab_name: str, logger: logging.Logger) -> pd.DataFrame:
    logger.info(f"Fetching sheet tab: {tab_name}")
    ws = client.open_by_key(sheet_key).worksheet(tab_name)
    records = ws.get_all_records()
    df = pd.DataFrame(records)
    logger.info(f"Loaded {len(df)} rows from {tab_name}")
    return df


def export_df_to_sheet(client: gspread.Client, sheet_url: str, tab_name: str, df: pd.DataFrame, logger: logging.Logger) -> None:
    logger.info(f"Exporting {len(df)} rows to tab: {tab_name}")
    sheet = client.open_by_url(sheet_url)
    try:
        ws = sheet.worksheet(tab_name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows="2000", cols="60")

    if df.empty:
        if len(df.columns) > 0:
            ws.update([df.columns.values.tolist()])
        else:
            ws.update([[]])
        logger.warning(f"Tab {tab_name} updated with empty dataset")
        return
    # Ensure JSON-serializable values for Google Sheets API
    df_clean = df.copy()
    df_clean = df_clean.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], "")
    df_clean = df_clean.applymap(
        lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else x
    )

    ws.update([df_clean.columns.values.tolist()] + df_clean.values.tolist())
    logger.info(f"Tab {tab_name} updated successfully")


# -----------------------------
# ClickUp Industry Mapping
# -----------------------------

class ClickUpIndustryFetcher:
    INDUSTRY_FIELD_ID = "bb224045-37ed-44b6-8204-dc42f32a44cd"

    def __init__(self, api_token: str, list_id: str, logger: logging.Logger):
        self.api_token = api_token
        self.list_id = list_id
        self.logger = logger
        self.headers = {
            "Authorization": self.api_token,
            "Content-Type": "application/json",
        }
        self.industry_map: Dict[str, str] = {}
        self.industry_order: List[str] = []
        self.tasks: List[Dict] = []

    def fetch_dropdown_options(self) -> None:
        url = f"https://api.clickup.com/api/v2/list/{self.list_id}/field"
        r = requests.get(url, headers=self.headers, timeout=30)
        if r.status_code != 200:
            self.logger.error(f"ClickUp field fetch failed | Status={r.status_code}")
            return

        for f in r.json().get("fields", []):
            if f.get("id") == self.INDUSTRY_FIELD_ID:
                opts = f.get("type_config", {}).get("options", [])
                self.industry_map = {str(o["id"]): o["name"] for o in opts}
                self.industry_order = [str(o["id"]) for o in opts]
                self.logger.info(f"Industry dropdown loaded | Options={len(self.industry_map)}")
                return

        self.logger.warning("Industry dropdown field not found in ClickUp")

    def fetch_tasks(self, limit: int = 100) -> None:
        page = 0
        while True:
            url = f"https://api.clickup.com/api/v2/list/{self.list_id}/task"
            params = {"page": page, "limit": limit, "include_closed": True, "include_archived": True}
            r = requests.get(url, headers=self.headers, params=params, timeout=30)

            if r.status_code != 200:
                self.logger.error(f"Task fetch failed | Page={page} | Status={r.status_code}")
                break

            tasks = r.json().get("tasks", [])
            self.tasks.extend(tasks)

            if len(tasks) < limit:
                break
            page += 1

        self.logger.info(f"ClickUp tasks fetched | Count={len(self.tasks)}")

    def _extract_dropdown_value(self, field: Dict) -> str:
        val = field.get("value")

        if isinstance(val, dict):
            if "name" in val and val["name"]:
                return str(val["name"])
            if "id" in val and str(val["id"]) in self.industry_map:
                return self.industry_map[str(val["id"])]

        if isinstance(val, str) and val in self.industry_map:
            return self.industry_map[val]

        if isinstance(val, int):
            if 0 <= val < len(self.industry_order):
                opt_id = self.industry_order[val]
                return self.industry_map.get(opt_id, "Not Set")

        return "Not Set"

    def build_industry_dataframe(self) -> pd.DataFrame:
        records = []
        for t in self.tasks:
            industry = "Not Set"
            for f in t.get("custom_fields", []):
                if f.get("id") == self.INDUSTRY_FIELD_ID:
                    industry = self._extract_dropdown_value(f)
                    break
            records.append({"Project Batch": t.get("name", ""), "Industry Type": industry})

        df = pd.DataFrame(records)
        self.logger.info(f"Industry mapping created | Rows={len(df)}")
        return df


# -----------------------------
# Transformations
# -----------------------------

def build_project_report(project_data: pd.DataFrame, internal_log_data: pd.DataFrame, clickup_industry_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    project_cols = [
        "Project Batch", "Month", "Client Source", "START DATE", "COMPLETION DATE",
        "Tool TYPE (POLYGON, POLYLINE ETC)", "Industry Type", "Project", "DL",
        "Effective Work Hour", "Bonus", "Penalty", "Final Working Hour",
        "Accuracy", "Client Billing Hours", "Resource Type", "Type"
    ]

    df = project_data.copy()
    df = ensure_columns(df, project_cols, "")

    for col in ["Effective Work Hour", "Bonus", "Penalty", "Final Working Hour", "Accuracy", "Client Billing Hours"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Accuracy"] = df["Accuracy"].apply(accuracy_to_ratio)
    df["Resource Type"] = df["Resource Type"].astype(str).str.strip().str.upper()

    df = df.drop(columns=["Industry Type"], errors="ignore")
    df["Project Batch"] = df["Project Batch"].astype(str)
    df = df.merge(clickup_industry_df, on="Project Batch", how="left")
    df["Industry Type"] = df["Industry Type"].fillna("Not Set")

    group_cols = [
        "Project Batch", "Month", "Client Source", "START DATE", "COMPLETION DATE",
        "Tool TYPE (POLYGON, POLYLINE ETC)", "Industry Type", "Project", "DL"
    ]

    project_pivot = (
        df.groupby(group_cols, dropna=False)
        .agg({
            "Effective Work Hour": "sum",
            "Bonus": "sum",
            "Penalty": "sum",
            "Final Working Hour": "sum",
            "Accuracy": "mean",
            "Client Billing Hours": "sum",
        })
        .reset_index()
        .rename(columns={
            "Effective Work Hour": "SUM of Effective Work Hour",
            "Bonus": "SUM of Bonus",
            "Penalty": "SUM of Penalty",
            "Final Working Hour": "SUM of Final Working Hour",
            "Accuracy": "AVERAGE of Accuracy",
            "Client Billing Hours": "SUM of Client Billing Hours",
        })
    )

    # Remote vs Inhouse
    remote_summary = (
        df.groupby(["Project Batch", "Resource Type"], dropna=False)["Final Working Hour"]
        .sum()
        .reset_index()
    )

    remote_agg = (
        remote_summary[remote_summary["Resource Type"] == "REMOTE"]
        .groupby("Project Batch")["Final Working Hour"]
        .sum()
        .reset_index()
        .rename(columns={"Final Working Hour": "Remote Hours"})
    )

    inhouse_agg = (
        remote_summary[remote_summary["Resource Type"] != "REMOTE"]
        .groupby("Project Batch")["Final Working Hour"]
        .sum()
        .reset_index()
        .rename(columns={"Final Working Hour": "Inhouse Hours"})
    )

    remote_summary_fixed = pd.merge(remote_agg, inhouse_agg, on="Project Batch", how="outer").fillna(0)

    # Internal log summary
    internal_cols = [
        "Project you worked on (Use Ctrl+F to search your required information)",
        "Annotation Time (Minutes)", "QA Time (Minutes)", "Crosscheck Time (Minutes)",
        "Meeting Time (Minutes)", "Project Study (Minutes)",
        "Resource Training (Minutes) - This section is for lead",
        "Q&A Group support (Minutes)", "Documentation (Minutes)", "Demo (Minutes)",
        "Break Time (Minutes)", "Server Downtime (Minutes)", "Free time (Minutes)",
    ]

    df_log = internal_log_data.copy()
    df_log = ensure_columns(df_log, internal_cols, 0)

    df_log["Project Batch"] = df_log[
        "Project you worked on (Use Ctrl+F to search your required information)"
    ].astype(str)

    minute_cols = [
        "Annotation Time (Minutes)", "QA Time (Minutes)", "Crosscheck Time (Minutes)",
        "Meeting Time (Minutes)", "Project Study (Minutes)",
        "Resource Training (Minutes) - This section is for lead",
        "Q&A Group support (Minutes)", "Documentation (Minutes)", "Demo (Minutes)",
        "Break Time (Minutes)", "Server Downtime (Minutes)", "Free time (Minutes)",
    ]

    for c in minute_cols:
        df_log[c] = pd.to_numeric(df_log[c], errors="coerce").fillna(0)

    df_log["Total Logged Minutes"] = df_log[minute_cols].sum(axis=1)
    df_log["Total Logged Hours"] = df_log["Total Logged Minutes"] / 60
    df_log["Internal Logged Hours (Anno+QA)"] = (df_log["Annotation Time (Minutes)"] + df_log["QA Time (Minutes)"]) / 60
    df_log["Other Hours (Excl. Anno+QA)"] = df_log["Total Logged Hours"] - df_log["Internal Logged Hours (Anno+QA)"]

    log_summary = (
        df_log.groupby("Project Batch")
        .agg({
            "Total Logged Hours": "sum",
            "Other Hours (Excl. Anno+QA)": "sum",
            "Internal Logged Hours (Anno+QA)": "sum",
        })
        .reset_index()
        .rename(columns={"Total Logged Hours": "Internal Logged Hours"})
    )

    # Type-wise hours
    df["Type"] = df["Type"].astype(str).str.upper()
    type_summary = (
        df.groupby(["Project Batch", "Type"])["Final Working Hour"]
        .sum()
        .reset_index()
    )
    type_pivot = (
        type_summary.pivot_table(index="Project Batch", columns="Type", values="Final Working Hour", fill_value=0)
        .reset_index()
    )
    type_pivot.columns.name = None
    type_pivot = type_pivot.rename(columns={
        "ANNOTATION": "Annotation Hours",
        "QC": "QC Hours",
        "TRACKING": "Tracking Hours",
    })

    final_df = (
        project_pivot.merge(remote_summary_fixed, on="Project Batch", how="left")
        .merge(log_summary, on="Project Batch", how="left")
        .merge(type_pivot, on="Project Batch", how="left")
        .fillna(0)
    )

    # Format dates
    for col in final_df.columns:
        final_df[col] = final_df[col].apply(lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else x)

    final_df = final_df.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], "").fillna("")
    logger.info(f"Project report prepared | Rows={len(final_df)}")
    return final_df


def build_merged_report(project_data: pd.DataFrame, internal_log_data: pd.DataFrame, clickup_industry_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    df_proj = project_data.copy()

    needed_cols = [
        "Project Batch", "Resource Type", "START DATE", "COMPLETION DATE",
        "Effective Work Hour", "Final Working Hour", "Client Billing Hours", "Bonus", "Penalty",
        "Accuracy", "Type", "QAI ID", "Full Name", "Client Source", "Industry Type",
        "Tool TYPE (POLYGON, POLYLINE ETC)", "Project", "Resource Allocation", "DL", "PDL",
    ]
    df_proj = ensure_columns(df_proj, needed_cols, "")

    df_proj["Project Batch"] = df_proj["Project Batch"].astype(str)
    df_proj["Resource Type"] = df_proj["Resource Type"].astype(str).str.upper()

    df_proj["START DATE"] = pd.to_datetime(df_proj["START DATE"], errors="coerce")
    df_proj["COMPLETION DATE"] = pd.to_datetime(df_proj["COMPLETION DATE"], errors="coerce")
    df_proj["COMPLETION DATE"] = df_proj["COMPLETION DATE"].fillna(df_proj["START DATE"])

    for c in ["Effective Work Hour", "Final Working Hour", "Client Billing Hours", "Bonus", "Penalty"]:
        df_proj[c] = pd.to_numeric(df_proj[c], errors="coerce").fillna(0)

    df_proj["Accuracy"] = df_proj["Accuracy"].apply(accuracy_to_ratio)

    df_proj = df_proj.drop(columns=["Industry Type"], errors="ignore")
    df_proj = df_proj.merge(clickup_industry_df, on="Project Batch", how="left")
    df_proj["Industry Type"] = df_proj["Industry Type"].fillna("Not Set")

    # Split hours across months
    def split_hours(row):
        start, end = row["START DATE"], row["COMPLETION DATE"]
        if pd.isna(start) or pd.isna(end) or end < start:
            return []

        total_days = (end - start).days + 1
        rows = []
        cursor = start

        while cursor <= end:
            m_start = cursor.replace(day=1)
            m_end = m_start + MonthEnd(1)

            p_start = max(cursor, start)
            p_end = min(m_end, end)
            ratio = ((p_end - p_start).days + 1) / total_days

            r = row.copy()
            r["REPORT_MONTH"] = m_start.strftime("%Y-%m")
            r["Effective Work Hour"] *= ratio
            r["Final Working Hour"] *= ratio
            rows.append(r)

            cursor = m_end + pd.Timedelta(days=1)

        return rows

    expanded = []
    for _, r in df_proj.iterrows():
        expanded.extend(split_hours(r))

    df_proj = pd.DataFrame(expanded)

    group_cols = [
        "REPORT_MONTH", "START DATE", "COMPLETION DATE", "Project Batch", "QAI ID", "Full Name",
        "Client Source", "Industry Type", "Tool TYPE (POLYGON, POLYLINE ETC)", "Project",
        "Resource Type", "Resource Allocation", "DL", "PDL",
    ]

    if df_proj.empty:
        logger.warning("No rows after month-splitting for merged report")
        return df_proj

    df_proj = (
        df_proj.groupby(group_cols, dropna=False)
        .agg({
            "Effective Work Hour": "sum",
            "Final Working Hour": "sum",
            "Bonus": "sum",
            "Penalty": "sum",
            "Accuracy": "mean",
            "Client Billing Hours": "sum",
        })
        .reset_index()
    )

    # Internal log data
    df_log = internal_log_data.copy()
    df_log = df_log.rename(columns={
        "Project you worked on (Use Ctrl+F to search your required information)": "Project Batch",
        "QAI ID (Use Ctrl+F to search your required information)": "QAI ID",
    })

    minute_cols = [
        "Annotation Time (Minutes)", "QA Time (Minutes)", "Crosscheck Time (Minutes)",
        "Meeting Time (Minutes)", "Project Study (Minutes)",
        "Resource Training (Minutes) - This section is for lead",
        "Q&A Group support (Minutes)", "Documentation (Minutes)", "Demo (Minutes)",
        "Break Time (Minutes)", "Server Downtime (Minutes)", "Free time (Minutes)",
    ]

    df_log = ensure_columns(df_log, minute_cols + ["Project Batch", "QAI ID"], 0)

    for c in minute_cols:
        df_log[c] = pd.to_numeric(df_log[c], errors="coerce").fillna(0) / 60

    df_log["Total Logged Hours"] = df_log[minute_cols].sum(axis=1)

    log_grouped = (
        df_log.groupby(["Project Batch", "QAI ID"], dropna=False)
        .agg({**{c: "sum" for c in minute_cols}, "Total Logged Hours": "sum"})
        .reset_index()
    )

    merged = df_proj.merge(log_grouped, on=["Project Batch", "QAI ID"], how="left")

    for c in merged.columns:
        if merged[c].dtype == "datetime64[ns]":
            merged[c] = merged[c].dt.strftime("%Y-%m-%d")

    merged = merged.fillna("")
    logger.info(f"Merged report prepared | Rows={len(merged)}")
    return merged


# -----------------------------
# Main
# -----------------------------

def _maybe_load_dotenv(logger: logging.Logger) -> None:
    if os.getenv("GITHUB_ACTIONS") == "true":
        return
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        logger.debug("python-dotenv not installed; skipping .env load")
        return
    load_dotenv()
    logger.info("Loaded environment from .env (local)")


def _maybe_load_dotenv(logger: logging.Logger) -> None:
    if os.getenv("GITHUB_ACTIONS") == "true":
        return
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        logger.debug("python-dotenv not installed; skipping .env load")
        return
    load_dotenv()
    logger.info("Loaded environment from .env (local)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Monthly Hours Report ETL")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    parser.add_argument("--log-file", default=os.getenv("LOG_FILE"))
    args = parser.parse_args()

    logger = setup_logging(args.log_level, args.log_file)
    logger.info("ETL job started")

    _maybe_load_dotenv(logger)

    creds_path = get_env("GOOGLE_CREDS_FILE", required=True)
    project_sheet_key = get_env("PROJECT_SHEET_KEY", required=True)
    project_tab = get_env("PROJECT_TAB", "For Internal Report")
    internal_sheet_key = get_env("INTERNAL_LOG_SHEET_KEY", required=True)
    internal_tab = get_env("INTERNAL_LOG_TAB", "Form Responses 1")

    clickup_token = get_env("CLICKUP_API_TOKEN", required=True)
    clickup_list_id = get_env("CLICKUP_LIST_ID", required=True)

    output_sheet_url = get_env("OUTPUT_SHEET_URL", required=True)
    project_report_tab = get_env("PROJECT_REPORT_TAB", "Project Report")
    merged_tab = get_env("MERGED_REPORT_TAB", "Merged")

    client = build_gspread_client(creds_path)

    project_data = fetch_sheet_df(client, project_sheet_key, project_tab, logger)
    internal_log_data = fetch_sheet_df(client, internal_sheet_key, internal_tab, logger)

    fetcher = ClickUpIndustryFetcher(clickup_token, clickup_list_id, logger)
    fetcher.fetch_dropdown_options()
    fetcher.fetch_tasks()
    clickup_industry_df = fetcher.build_industry_dataframe()

    project_report = build_project_report(project_data, internal_log_data, clickup_industry_df, logger)
    merged_report = build_merged_report(project_data, internal_log_data, clickup_industry_df, logger)

    export_df_to_sheet(client, output_sheet_url, project_report_tab, project_report, logger)
    export_df_to_sheet(client, output_sheet_url, merged_tab, merged_report, logger)

    logger.info("ETL job completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
