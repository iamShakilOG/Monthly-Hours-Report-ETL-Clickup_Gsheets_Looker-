# Monthly Hours Report ETL

Automates monthly reporting by pulling data from Google Sheets, enriching it with ClickUp industry values, and exporting two tabs:
- `Project Report`
- `Merged`

## What It Does
- Reads project data and internal log data from Google Sheets
- Fetches industry dropdown values from ClickUp
- Builds two reporting outputs
- Writes results to a target Google Sheet

## Local Setup
1. Create a service account in Google Cloud and download the JSON file.
2. Share the source and output Google Sheets with the service account email.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Create a `.env` (copy from `.env.example`) and fill in values.

5. Run the ETL:

```bash
python Monthly_hours_Report_ETL.py
```

## Environment Variables
See `.env.example` for the full list. Key variables:
- `GOOGLE_CREDS_FILE`
- `PROJECT_SHEET_KEY`
- `INTERNAL_LOG_SHEET_KEY`
- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `OUTPUT_SHEET_URL`

## GitHub Actions
Workflow file: `.github/workflows/monthly_hours_etl.yml`

### Required GitHub Secrets
- `GOOGLE_CREDS_JSON` (entire service account JSON as a single line)
- `PROJECT_SHEET_KEY`
- `PROJECT_TAB`
- `INTERNAL_LOG_SHEET_KEY`
- `INTERNAL_LOG_TAB`
- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `OUTPUT_SHEET_URL`
- `PROJECT_REPORT_TAB`
- `MERGED_REPORT_TAB`

## Security Notes
- Do not commit `.env` or any service account JSON file.
- `.gitignore` already excludes `.env` and credential files.
- Use GitHub Secrets for production runs.

## Troubleshooting
- If you see `Missing required env var`, make sure `.env` is present locally or your environment variables are set.
- For local runs, `.env` is loaded automatically if `python-dotenv` is installed.

## License
Add a license if you plan to distribute this repository.
