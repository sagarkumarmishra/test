https://datahub-nonprod.xxx.allianz.de
USERNAME = "adp_service_user"
PASSWORD = "********"


# ================================
# ADP → DataHub API Connectivity Test
# Azure Synapse Python Notebook
# ================================

import requests
import json
import time

# ---------- CONFIGURATION ----------
BASE_URL = "https://<DATAHUB_HOST>"
USERNAME = "<DATAHUB_USERNAME>"
PASSWORD = "<DATAHUB_PASSWORD>"

CSV_FILE_PATH = "/tmp/aspire_test.csv"  # temporary test file
# -----------------------------------

# ---------- STEP 1: Create dummy Aspire CSV ----------
csv_content = """account_id,exposure
ACC001,1000
ACC002,2000
"""

with open(CSV_FILE_PATH, "w") as f:
    f.write(csv_content)

print("✅ Dummy Aspire CSV created")

# ---------- STEP 2: Get Auth Token ----------
token_url = BASE_URL + "/api/v1/auth/token"

token_response = requests.post(
    token_url,
    data={
        "username": USERNAME,
        "password": PASSWORD
    }
)

if token_response.status_code != 200:
    raise Exception("❌ Token generation failed")

access_token = token_response.json()["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}

print("✅ Authentication successful")

# ---------- STEP 3: Upload File ----------
upload_url = BASE_URL + "/api/v1/file/upload"

with open(CSV_FILE_PATH, "rb") as f:
    upload_response = requests.post(
        upload_url,
        headers=headers,
        files={"file": f}
    )

if upload_response.status_code != 200:
    raise Exception("❌ File upload failed")

file_id = upload_response.json()["fileId"]
print(f"✅ File uploaded | fileId: {file_id}")

# ---------- STEP 4: Add Metadata ----------
metadata_url = BASE_URL + f"/api/v1/file/{file_id}/metadata"

metadata_payload = {
    "sourceSystem": "ADP",
    "fileType": "CIM",
    "format": "CSV"
}

metadata_response = requests.post(
    metadata_url,
    headers=headers,
    json=metadata_payload
)

if metadata_response.status_code not in [200, 201]:
    raise Exception("❌ Metadata update failed")

print("✅ Metadata added")

# ---------- STEP 5: Trigger Ingestion ----------
ingestion_url = BASE_URL + "/api/process/v1/portfolio/ingestion"

ingestion_response = requests.post(
    ingestion_url,
    headers=headers,
    json={"fileId": file_id}
)

if ingestion_response.status_code not in [200, 201]:
    raise Exception("❌ Ingestion trigger failed")

ingestion_id = ingestion_response.json()["ingestionId"]
print(f"✅ Ingestion started | ingestionId: {ingestion_id}")

# ---------- STEP 6: Check Status ----------
time.sleep(5)

status_url = BASE_URL + f"/api/process/v1/portfolio/ingestion/{ingestion_id}"
status_response = requests.get(status_url, headers=headers)

print("📌 Ingestion Status:")
print(json.dumps(status_response.json(), indent=2))

# ---------- STEP 7: Get Result ----------
result_url = BASE_URL + f"/api/process/v1/portfolio/ingestion/{ingestion_id}/result"
result_response = requests.get(result_url, headers=headers)

print("📊 Ingestion Result:")
print(json.dumps(result_response.json(), indent=2))

print("🎯 ADP → DataHub connectivity validated successfully")
