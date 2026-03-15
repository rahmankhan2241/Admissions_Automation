import os
import requests
import pandas as pd
from google.cloud import bigquery

# ==========================================
# 0. AUTHENTICATION SETUP
# ==========================================
# Dynamically find the key.json file in the same folder as this script
current_directory = os.path.dirname(os.path.abspath(__file__))
key_path = os.path.join(current_directory, 'key.json')

# Tell Google Cloud to use this key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# ==========================================
# 1. CONFIGURATION
# ==========================================
PROJECT_ID = 'k12-project-489714'
DATASET_ID = 'inventory_data'
TABLE_ID = 'Admissions_Data'
FULL_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

LOGIN_URL = 'https://orchids.letseduvate.com/qbox/erp_user/user-mgmt/staff-login/'
REPORT_URL = 'https://orchids.finance.letseduvate.com/qbox/apiV1/report/admission-report/'

# ⚠️ Replace these with your actual credentials ⚠️
USERNAME = "20250003042_OIS"
PASSWORD = "R@hman2241"

# Specific Columns Required
REQUIRED_COLUMNS = ["admission_date", "erp_id", "branch_name", "grade_name"]

# Branch IDs string
BRANCH_IDS = '69,283,8,24,245,291,290,363,301,367,299,285,360,252,70,242,254,77,249,67,272,4,275,286,20,265,7,357,251,287,282,15,253,264,246,277,21,41,268,66,239,292,280,269,354,210,30,3,194,19,241,213,17,73,101,82,338,274,355,250,12,362,267,94,76,124,305,57,361,297,258,6,205,27,289,10,298,300,13,353,359,271,276,18,81,123,364,240,366,266,288,14,11,356,270,365,72,296,273,248,358,257,281,5,293,244,209,26,369,371,370,423,425,428,427,426,429,430,432,433,436,435,434,438,437'

# Academic Years (Ignoring 24-25 / Year ID 9)
YEAR_CONFIGS = [
    {"year_id": "42", "start": "2024-01-01", "end": "2026-12-31", "label": "25-26"},
    {"year_id": "47", "start": "2024-01-01", "end": "2026-12-31", "label": "26-27"}
]

# ==========================================
# 2. CORE FUNCTIONS
# ==========================================

def get_access_token():
    payload = {"username": USERNAME, "password": PASSWORD}
    response = requests.post(LOGIN_URL, json=payload)
    if response.status_code not in [200, 201]:
        raise Exception(f"Login failed: {response.text}")
    return response.json().get('result', {}).get('access')

def fetch_and_filter(token, config):
    params = {
        "finance_session_year_id": config['year_id'],
        "start_date": config['start'],
        "end_date": config['end'],
        "branch_id": BRANCH_IDS
    }
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    
    print(f"Fetching Year: {config['label']}...")
    response = requests.get(REPORT_URL, params=params, headers=headers)
    
    if response.status_code != 200:
        print(f"Error fetching {config['label']}: {response.status_code}")
        return []

    data = response.json().get('data', [])
    
    # Filter only for the 4 required keys
    filtered_data = []
    for row in data:
        filtered_row = {col: row.get(col, "") for col in REQUIRED_COLUMNS}
        filtered_data.append(filtered_row)
        
    return filtered_data

# ==========================================
# 3. BIGQUERY UPLOAD RUNNER
# ==========================================

def start_sync():
    token = get_access_token()
    all_rows = []

    for config in YEAR_CONFIGS:
        rows = fetch_and_filter(token, config)
        all_rows.extend(rows)

    if not all_rows:
        print("No records found.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)

    # 1. Force erp_id to string to prevent BQ from guessing it's an integer
    df['erp_id'] = df['erp_id'].astype(str)
    
    # 2. Convert admission_date to proper DateTime object (handles messy API dates)
    df['admission_date'] = pd.to_datetime(df['admission_date'], errors='coerce')

    # Initialize BigQuery Client (It will automatically use the key.json now)
    client = bigquery.Client(project=PROJECT_ID)
    
    # 3. Explicitly define the Schema
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("admission_date", "DATE"),
            bigquery.SchemaField("erp_id", "STRING"),
            bigquery.SchemaField("branch_name", "STRING"),
            bigquery.SchemaField("grade_name", "STRING"),
        ],
        # WRITE_TRUNCATE: Creates table if missing, Replaces data if exists
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Uploading {len(df)} records to BigQuery...")
    print("If table doesn't exist, it will be created automatically.")
    
    job = client.load_table_from_dataframe(df, FULL_TABLE_PATH, job_config=job_config)
    job.result()  # Wait for the job to finish

    print(f"Success! {FULL_TABLE_PATH} is updated and ready.")

if __name__ == "__main__":
    start_sync()