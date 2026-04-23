# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f07fe9f1-7b3c-4999-b710-ecae0e773e47",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f",
# META       "known_lakehouses": [
# META         {
# META           "id": "f07fe9f1-7b3c-4999-b710-ecae0e773e47"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import pandas as pd

token = "YOUR_ACCESS_TOKEN"
workspace_id = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"

headers = {"Authorization": f"Bearer {token}"}

# Get reports
reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
reports = requests.get(reports_url, headers=headers).json()["value"]

data = []

for r in reports:
    dataset_id = r.get("datasetId")
    
    dataset_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
    dataset_info = requests.get(dataset_url, headers=headers).json()
    
    data.append({
        "workspace_name": "ICICI_DSAG",
        "report_name": r.get("name"),
        "report_id": r.get("id"),
        "dataset_id": dataset_id,
        "developer": dataset_info.get("configuredBy")
    })

df = spark.createDataFrame(pd.DataFrame(data))
df.write.mode("overwrite").saveAsTable("reports_inventory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

# Fabric provides token automatically
from notebookutils import mssparkutils

token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

workspace_id = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"

headers = {
    "Authorization": f"Bearer {token}"
}

# Get Reports
reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
reports = requests.get(reports_url, headers=headers).json()["value"]

data = []

for r in reports:
    data.append({
        "report_name": r.get("name"),
        "report_id": r.get("id"),
        "dataset_id": r.get("datasetId")
    })

data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_data = []

for r in reports:
    dataset_id = r.get("datasetId")
    
    dataset_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
    ds = requests.get(dataset_url, headers=headers).json()
    
    final_data.append({
        "report_name": r.get("name"),
        "report_id": r.get("id"),
        "dataset_id": dataset_id,
        "developer": ds.get("configuredBy")
    })

final_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
workspace_info = requests.get(workspace_url, headers=headers).json()

workspace_info

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

users_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"
users = requests.get(users_url, headers=headers).json()["value"]

users_data = []

for u in users:
    users_data.append({
        "user_email": u.get("emailAddress"),
        "role": u.get("groupUserAccessRight")
    })

users_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dash_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dashboards"
dashboards = requests.get(dash_url, headers=headers).json()["value"]

dash_data = []

for d in dashboards:
    dash_data.append({
        "dashboard_name": d.get("displayName"),
        "dashboard_id": d.get("id")
    })

dash_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

users_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"
users = requests.get(users_url, headers=headers).json()["value"]

users_data = []

for u in users:
    users_data.append({
        "user_email": u.get("emailAddress"),
        "role": u.get("groupUserAccessRight")
    })

users_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

df_users = spark.createDataFrame(pd.DataFrame(users_data))
df_users.write.mode("overwrite").saveAsTable("workspace_users")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from workspace_users;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Users
df_users = spark.createDataFrame(pd.DataFrame(users_data))
df_users.write.mode("overwrite").saveAsTable("workspace_users")

# Dashboards
df_dash = spark.createDataFrame(pd.DataFrame(dash_data))
df_dash.write.mode("overwrite").saveAsTable("dashboard_inventory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dash_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dashboards"
response = requests.get(dash_url, headers=headers).json()

dashboards = response.get("value", [])

dash_data = []

if len(dashboards) == 0:
    print("No dashboards found in this workspace")
else:
    for d in dashboards:
        dash_data.append({
            "dashboard_name": d.get("displayName"),
            "dashboard_id": d.get("id")
        })

dash_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

if len(dash_data) > 0:
    df_dash = spark.createDataFrame(pd.DataFrame(dash_data))
    df_dash.write.mode("overwrite").saveAsTable("dashboard_inventory")
    display(df_dash)
else:
    print("No dashboard data to save")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

# Auto token from Fabric
from notebookutils import mssparkutils
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

workspace_id = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"

headers = {
    "Authorization": f"Bearer {token}"
}

# =========================
# STEP 1: REPORTS + DEVELOPERS
# =========================
reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
reports = requests.get(reports_url, headers=headers).json().get("value", [])

final_data = []

for r in reports:
    dataset_id = r.get("datasetId")
    
    dataset_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
    ds = requests.get(dataset_url, headers=headers).json()
    
    final_data.append({
        "workspace_name": "ICICI_DSAG",
        "report_name": r.get("name"),
        "report_id": r.get("id"),
        "dataset_id": dataset_id,
        "developer": ds.get("configuredBy")
    })

# =========================
# STEP 2: USERS + ROLES
# =========================
users_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"
users = requests.get(users_url, headers=headers).json().get("value", [])

users_data = []

for u in users:
    users_data.append({
        "user_email": u.get("emailAddress"),
        "role": u.get("groupUserAccessRight")
    })

# =========================
# STEP 3: COMBINE (CROSS JOIN)
# =========================
df_reports_pd = pd.DataFrame(final_data)
df_users_pd = pd.DataFrame(users_data)

df_reports_pd["key"] = 1
df_users_pd["key"] = 1

final_df = pd.merge(df_reports_pd, df_users_pd, on="key").drop("key", axis=1)

# =========================
# STEP 4: SAVE TO LAKEHOUSE
# =========================
spark_df = spark.createDataFrame(final_df)
spark_df.write.mode("overwrite").saveAsTable("final_governance_table")

# Display result
display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

# Fabric token
from notebookutils import mssparkutils
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

workspace_id = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"

headers = {
    "Authorization": f"Bearer {token}"
}

# =========================
# GET REPORTS (BASIC)
# =========================
reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
response = requests.get(reports_url, headers=headers).json()

reports = response.get("value", [])

data = []

for r in reports:
    data.append({
        "workspace_id": workspace_id,
        "report_name": r.get("name"),
        "report_id": r.get("id"),
        "dataset_id": r.get("datasetId"),
        "web_url": r.get("webUrl")
    })

# Convert to Spark DF
df = spark.createDataFrame(pd.DataFrame(data))

# Save table
df.write.mode("overwrite").saveAsTable("reports_basic_inventory")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

# Fabric token
from notebookutils import mssparkutils
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

workspace_id = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"

headers = {
    "Authorization": f"Bearer {token}"
}

# =========================
# GET REPORTS + DATASET OWNER
# =========================
reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
reports = requests.get(reports_url, headers=headers).json().get("value", [])

data = []

for r in reports:
    dataset_id = r.get("datasetId")
    
    # Get dataset info (developer)
    dataset_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
    ds = requests.get(dataset_url, headers=headers).json()
    
    data.append({
        "workspace_name": "ICICI_DSAG",
        "workspace_id": workspace_id,
        "report_name": r.get("name"),
        "report_id": r.get("id"),
        "dataset_id": dataset_id,
        "dataset_owner": ds.get("configuredBy")   # ⭐ key difference
    })

# Convert to Spark DF
df = spark.createDataFrame(pd.DataFrame(data))

# Save table
df.write.mode("overwrite").saveAsTable("reports_with_owner")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

# Fabric token
from notebookutils import mssparkutils
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

workspace_id = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"

headers = {
    "Authorization": f"Bearer {token}"
}

# =========================
# STEP 1: GET REPORTS + DATASET OWNER
# =========================
reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
reports = requests.get(reports_url, headers=headers).json().get("value", [])

reports_data = []

for r in reports:
    dataset_id = r.get("datasetId")
    
    dataset_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
    ds = requests.get(dataset_url, headers=headers).json()
    
    reports_data.append({
        "workspace_name": "ICICI_DSAG",
        "workspace_id": workspace_id,
        "report_name": r.get("name"),
        "report_id": r.get("id"),
        "dataset_id": dataset_id,
        "dataset_owner": ds.get("configuredBy")
    })

# =========================
# STEP 2: GET USERS + ROLES
# =========================
users_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"
users = requests.get(users_url, headers=headers).json().get("value", [])

users_data = []

for u in users:
    users_data.append({
        "user_email": u.get("emailAddress"),
        "role": u.get("groupUserAccessRight")
    })

# =========================
# STEP 3: CROSS JOIN (COMBINE)
# =========================
df_reports = pd.DataFrame(reports_data)
df_users = pd.DataFrame(users_data)

df_reports["key"] = 1
df_users["key"] = 1

final_df = pd.merge(df_reports, df_users, on="key").drop("key", axis=1)

# =========================
# STEP 4: SAVE
# =========================
spark_df = spark.createDataFrame(final_df)

spark_df.write.mode("overwrite").saveAsTable("final_governance_table")

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# ICICI Bank - Power BI / Fabric Report Metadata Extractor
# Method 2: Microsoft Fabric PySpark Notebook
# Workspace: ICICI_DSAG
# Output   : Lakehouse CSV + DataFrame display
# =============================================================================
#
# PRE-REQUISITES:
#   1. This notebook must run inside Microsoft Fabric (not standalone Spark)
#   2. Attach this notebook to a Lakehouse
#   3. The Fabric workspace identity (MSI) or your user account must have:
#      - Power BI Admin role  (for /admin/* APIs)  — OR —
#      - Workspace Contributor role (for non-admin APIs)
#   4. Enable "Service Principal can use Fabric APIs" in Fabric Admin Portal
#      if using Service Principal authentication
#
# HOW TO RUN:
#   - Upload this .py file as a new Fabric Notebook
#   - Set WORKSPACE_NAME in Cell 1
#   - Run All Cells
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — CONFIGURATION  (Edit these values)
# ─────────────────────────────────────────────────────────────────────────────

WORKSPACE_NAME = "Batch_workspace"

# Output paths in Lakehouse Files section
REPORT_OUTPUT_CSV    = "Files/icici_dsag_report_audit.csv"
DASHBOARD_OUTPUT_CSV = "Files/icici_dsag_dashboard_audit.csv"

# Authentication mode: "msi" = Fabric Managed Identity (recommended in Fabric)
#                      "sp"  = Service Principal (set SP_* vars below)
#                      "pat" = Personal Access Token (dev/testing only)
AUTH_MODE = "msi"

# Service Principal credentials (only if AUTH_MODE = "sp")
SP_TENANT_ID     = ""
SP_CLIENT_ID     = ""
SP_CLIENT_SECRET = ""

# Personal Access Token (only if AUTH_MODE = "pat" — NOT for production)
PAT_TOKEN = ""


# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — IMPORTS & HELPER SETUP
# ─────────────────────────────────────────────────────────────────────────────

import requests
import json
import pandas as pd
from datetime import datetime

# Fabric / Synapse token helper
try:
    from notebookutils import mssparkutils
    IN_FABRIC = True
except ImportError:
    IN_FABRIC = False
    print("[WARN] notebookutils not found — assuming local/testing mode")

POWERBI_BASE     = "https://api.powerbi.com/v1.0/myorg"
POWERBI_ADMIN    = "https://api.powerbi.com/v1.0/myorg/admin"
RESOURCE_URL     = "https://analysis.windows.net/powerbi/api"


def get_access_token():
    """Obtain Bearer token based on AUTH_MODE."""
    if AUTH_MODE == "msi" and IN_FABRIC:
        # Use Fabric built-in MSI token provider
        token = mssparkutils.credentials.getToken(RESOURCE_URL)
        return token

    elif AUTH_MODE == "sp":
        url = f"https://login.microsoftonline.com/{SP_TENANT_ID}/oauth2/v2.0/token"
        data = {
            "grant_type"   : "client_credentials",
            "client_id"    : SP_CLIENT_ID,
            "client_secret": SP_CLIENT_SECRET,
            "scope"        : f"{RESOURCE_URL}/.default"
        }
        resp = requests.post(url, data=data)
        resp.raise_for_status()
        return resp.json()["access_token"]

    elif AUTH_MODE == "pat":
        return PAT_TOKEN

    else:
        raise ValueError(f"Unknown AUTH_MODE: {AUTH_MODE}")


def make_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type" : "application/json"
    }


def safe_get(url, headers, params=None):
    """GET request with basic error handling; returns parsed JSON or None."""
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        print(f"  [WARN] HTTP {e.response.status_code} for {url}: {e.response.text[:200]}")
        return None
    except Exception as e:
        print(f"  [WARN] Request failed for {url}: {e}")
        return None


print("[SETUP] Libraries loaded successfully.")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — AUTHENTICATE
# ─────────────────────────────────────────────────────────────────────────────

print(f"[AUTH] Authenticating using mode: {AUTH_MODE} ...")
TOKEN   = get_access_token()
HEADERS = make_headers(TOKEN)
print("[AUTH] ✅ Authentication successful.\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — RESOLVE WORKSPACE
# ─────────────────────────────────────────────────────────────────────────────

print(f"[INFO] Resolving workspace: '{WORKSPACE_NAME}' ...")

# Try regular API first
ws_resp = safe_get(
    f"{POWERBI_BASE}/groups",
    HEADERS,
    params={"$filter": f"name eq '{WORKSPACE_NAME}'", "$top": 5}
)

workspace = None
if ws_resp and ws_resp.get("value"):
    workspace = ws_resp["value"][0]
else:
    # Fallback: Admin API (requires Power BI Admin role)
    ws_resp = safe_get(
        f"{POWERBI_ADMIN}/groups",
        HEADERS,
        params={"$filter": f"name eq '{WORKSPACE_NAME}'", "$top": 5}
    )
    if ws_resp and ws_resp.get("value"):
        workspace = ws_resp["value"][0]

if not workspace:
    raise RuntimeError(
        f"Workspace '{WORKSPACE_NAME}' not found. "
        "Check name and permissions."
    )

WORKSPACE_ID   = workspace["id"]
IS_DEDICATED   = workspace.get("isOnDedicatedCapacity", False)
WORKSPACE_TYPE = workspace.get("type", "Workspace")

print(f"[INFO] ✅ Found workspace: {WORKSPACE_NAME}")
print(f"       ID             : {WORKSPACE_ID}")
print(f"       Type           : {WORKSPACE_TYPE}")
print(f"       Dedicated Cap. : {IS_DEDICATED}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — FETCH DATASETS (for developer/owner info)
# ─────────────────────────────────────────────────────────────────────────────

print("[INFO] Fetching datasets ...")
ds_resp = safe_get(f"{POWERBI_BASE}/groups/{WORKSPACE_ID}/datasets", HEADERS)
datasets_raw = ds_resp.get("value", []) if ds_resp else []

dataset_map = {}
for ds in datasets_raw:
    dataset_map[ds["id"]] = {
        "name"        : ds.get("name", "N/A"),
        "configuredBy": ds.get("configuredBy", ds.get("createdBy", "N/A")),
        "createdDate" : ds.get("createdDate", "N/A"),
        "isRefreshable": ds.get("isRefreshable", False)
    }
print(f"[INFO] ✅ {len(dataset_map)} dataset(s) found.\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — FETCH WORKSPACE MEMBERS
# ─────────────────────────────────────────────────────────────────────────────

print("[INFO] Fetching workspace members ...")
mem_resp = safe_get(f"{POWERBI_BASE}/groups/{WORKSPACE_ID}/users", HEADERS)
members_raw = mem_resp.get("value", []) if mem_resp else []

# Build email -> {role, displayName} map
member_map = {}
for m in members_raw:
    email = m.get("emailAddress") or m.get("identifier", "")
    member_map[email.lower()] = {
        "role"       : m.get("groupUserAccessRight", "Viewer"),
        "displayName": m.get("displayName", email)
    }
print(f"[INFO] ✅ {len(member_map)} workspace member(s).\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — FETCH REPORTS
# ─────────────────────────────────────────────────────────────────────────────

print("[INFO] Fetching reports ...")
rpt_resp = safe_get(f"{POWERBI_BASE}/groups/{WORKSPACE_ID}/reports", HEADERS)
reports_raw = rpt_resp.get("value", []) if rpt_resp else []
print(f"[INFO] ✅ {len(reports_raw)} report(s) found.\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — DETERMINE ORG-SHARE STATUS
# ─────────────────────────────────────────────────────────────────────────────

def get_org_share_status(report_id):
    """
    Checks if a report is published as a Power BI App visible to the entire org.
    Returns: "Yes - Org App", "No", or "Unknown"
    """
    # Check Publish-to-Web embeds (org-level public access)
    embed_resp = safe_get(
        f"https://api.powerbi.com/v1.0/myorg/admin/reports/{report_id}/datasources",
        HEADERS
    )
    # Proxy: if workspace is on dedicated capacity and has an org app, mark as possibly shared
    if IS_DEDICATED:
        return "Possibly Yes (Dedicated Capacity Workspace)"
    return "No (Standard Workspace)"


# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — BUILD REPORT AUDIT DATAFRAME
# ─────────────────────────────────────────────────────────────────────────────

print("[INFO] Building report audit rows ...")
report_rows = []

for report in reports_raw:
    report_id   = report.get("id", "N/A")
    report_name = report.get("name", "N/A")
    dataset_id  = report.get("datasetId", "N/A")
    report_type = report.get("reportType", "PowerBIReport")
    web_url     = report.get("webUrl", "N/A")
    embed_url   = report.get("embedUrl", "N/A")
    created_dt  = report.get("createdDateTime", "N/A")
    modified_dt = report.get("modifiedDateTime", "N/A")
    modified_by = report.get("modifiedBy", "N/A")

    # Developer from dataset
    ds_info   = dataset_map.get(dataset_id, {})
    developer = ds_info.get("configuredBy", "N/A")

    # Org-share status
    shared_to_org = get_org_share_status(report_id)

    # Per-report users (Admin API)
    users = []
    user_resp = safe_get(
        f"{POWERBI_ADMIN}/reports/{report_id}/users",
        HEADERS
    )
    if user_resp and user_resp.get("value"):
        users = user_resp["value"]
    else:
        # Fallback to workspace members
        users = [
            {
                "emailAddress"          : email,
                "displayName"           : info["displayName"],
                "reportUserAccessRight" : info["role"]
            }
            for email, info in member_map.items()
        ]

    if users:
        for user in users:
            email        = user.get("emailAddress") or user.get("identifier", "N/A")
            display_name = user.get("displayName", email)
            role         = (
                user.get("reportUserAccessRight")
                or member_map.get(email.lower(), {}).get("role", "Viewer")
            )
            report_rows.append({
                "WorkspaceName"   : WORKSPACE_NAME,
                "WorkspaceId"     : WORKSPACE_ID,
                "ReportId"        : report_id,
                "ReportName"      : report_name,
                "DatasetId"       : dataset_id,
                "DatasetName"     : ds_info.get("name", "N/A"),
                "Developer"       : developer,
                "UserEmail"       : email,
                "UserDisplayName" : display_name,
                "UserRole"        : role,
                "SharedToOrg"     : shared_to_org,
                "ReportType"      : report_type,
                "WebUrl"          : web_url,
                "EmbedUrl"        : embed_url,
                "CreatedDateTime" : created_dt,
                "ModifiedDateTime": modified_dt,
                "ModifiedBy"      : modified_by,
                "AuditTimestamp"  : datetime.utcnow().isoformat()
            })
    else:
        report_rows.append({
            "WorkspaceName"   : WORKSPACE_NAME,
            "WorkspaceId"     : WORKSPACE_ID,
            "ReportId"        : report_id,
            "ReportName"      : report_name,
            "DatasetId"       : dataset_id,
            "DatasetName"     : ds_info.get("name", "N/A"),
            "Developer"       : developer,
            "UserEmail"       : "N/A",
            "UserDisplayName" : "N/A",
            "UserRole"        : "N/A",
            "SharedToOrg"     : shared_to_org,
            "ReportType"      : report_type,
            "WebUrl"          : web_url,
            "EmbedUrl"        : embed_url,
            "CreatedDateTime" : created_dt,
            "ModifiedDateTime": modified_dt,
            "ModifiedBy"      : modified_by,
            "AuditTimestamp"  : datetime.utcnow().isoformat()
        })

df_reports = pd.DataFrame(report_rows)
print(f"[INFO] ✅ Report audit: {len(df_reports)} row(s) built.")
print("\n── REPORTS PREVIEW ──────────────────────────────────────────────────────────")
display(df_reports.head(20))


# ─────────────────────────────────────────────────────────────────────────────
# CELL 10 — FETCH & BUILD DASHBOARD AUDIT DATAFRAME
# ─────────────────────────────────────────────────────────────────────────────

print("\n[INFO] Fetching dashboards ...")
dash_resp = safe_get(f"{POWERBI_BASE}/groups/{WORKSPACE_ID}/dashboards", HEADERS)
dashboards_raw = dash_resp.get("value", []) if dash_resp else []
print(f"[INFO] ✅ {len(dashboards_raw)} dashboard(s) found.\n")

dashboard_rows = []

for dash in dashboards_raw:
    dash_id    = dash.get("id", "N/A")
    dash_name  = dash.get("displayName", "N/A")
    is_readonly= dash.get("isReadOnly", False)
    embed_url  = dash.get("embedUrl", "N/A")
    web_url    = dash.get("webUrl", "N/A")

    # Tiles → linked report IDs
    tiles_resp = safe_get(
        f"{POWERBI_BASE}/groups/{WORKSPACE_ID}/dashboards/{dash_id}/tiles",
        HEADERS
    )
    linked_reports = "N/A"
    if tiles_resp and tiles_resp.get("value"):
        rpt_ids = list({
            t["reportId"] for t in tiles_resp["value"] if t.get("reportId")
        })
        linked_reports = "; ".join(rpt_ids) if rpt_ids else "N/A"

    # Dashboard users
    dash_users = []
    du_resp = safe_get(f"{POWERBI_ADMIN}/dashboards/{dash_id}/users", HEADERS)
    if du_resp and du_resp.get("value"):
        dash_users = du_resp["value"]
    else:
        dash_users = [
            {
                "emailAddress"              : email,
                "displayName"               : info["displayName"],
                "dashboardUserAccessRight"  : info["role"]
            }
            for email, info in member_map.items()
        ]

    if dash_users:
        for user in dash_users:
            email        = user.get("emailAddress") or user.get("identifier", "N/A")
            display_name = user.get("displayName", email)
            role         = (
                user.get("dashboardUserAccessRight")
                or member_map.get(email.lower(), {}).get("role", "Viewer")
            )
            # Mark as developer if Admin/Owner/Member
            is_developer = "Yes" if role in ("Admin", "Owner", "Member") else "No"

            dashboard_rows.append({
                "WorkspaceName"  : WORKSPACE_NAME,
                "WorkspaceId"    : WORKSPACE_ID,
                "DashboardId"    : dash_id,
                "DashboardName"  : dash_name,
                "Status"         : "ReadOnly" if is_readonly else "Active",
                "UserEmail"      : email,
                "UserDisplayName": display_name,
                "UserRole"       : role,
                "IsDeveloper"    : is_developer,
                "LinkedReportIds": linked_reports,
                "EmbedUrl"       : embed_url,
                "WebUrl"         : web_url,
                "AuditTimestamp" : datetime.utcnow().isoformat()
            })
    else:
        dashboard_rows.append({
            "WorkspaceName"  : WORKSPACE_NAME,
            "WorkspaceId"    : WORKSPACE_ID,
            "DashboardId"    : dash_id,
            "DashboardName"  : dash_name,
            "Status"         : "ReadOnly" if is_readonly else "Active",
            "UserEmail"      : "N/A",
            "UserDisplayName": "N/A",
            "UserRole"       : "N/A",
            "IsDeveloper"    : "N/A",
            "LinkedReportIds": linked_reports,
            "EmbedUrl"       : embed_url,
            "WebUrl"         : web_url,
            "AuditTimestamp" : datetime.utcnow().isoformat()
        })

df_dashboards = pd.DataFrame(dashboard_rows)
print(f"[INFO] ✅ Dashboard audit: {len(df_dashboards)} row(s) built.")
print("\n── DASHBOARDS PREVIEW ───────────────────────────────────────────────────────")
display(df_dashboards.head(20))


# ─────────────────────────────────────────────────────────────────────────────
# CELL 11 — SAVE TO LAKEHOUSE (CSV)
# ─────────────────────────────────────────────────────────────────────────────

print("\n[INFO] Saving CSVs to Lakehouse ...")

if IN_FABRIC:
    # In Fabric, write to the attached Lakehouse using Spark
    spark_df_reports    = spark.createDataFrame(df_reports)
    spark_df_dashboards = spark.createDataFrame(df_dashboards)

    spark_df_reports.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv(f"Files/icici_dsag_report_audit")

    spark_df_dashboards.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv(f"Files/icici_dsag_dashboard_audit")

    print(f"[SUCCESS] ✅ Reports CSV    → Lakehouse: Files/icici_dsag_report_audit/")
    print(f"[SUCCESS] ✅ Dashboards CSV → Lakehouse: Files/icici_dsag_dashboard_audit/")
else:
    # Local / testing fallback
    df_reports.to_csv("icici_dsag_report_audit.csv", index=False, encoding="utf-8-sig")
    df_dashboards.to_csv("icici_dsag_dashboard_audit.csv", index=False, encoding="utf-8-sig")
    print("[SUCCESS] ✅ Saved locally (testing mode).")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 12 — SUMMARY STATISTICS
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("   ICICI DSAG WORKSPACE — AUDIT SUMMARY")
print("="*60)
print(f"  Workspace      : {WORKSPACE_NAME} ({WORKSPACE_ID})")
print(f"  Reports found  : {len(reports_raw)}")
print(f"  Dashboards     : {len(dashboards_raw)}")
print(f"  Datasets       : {len(dataset_map)}")
print(f"  Workspace Users: {len(member_map)}")
print("="*60)

# Reports by org-share status
print("\n[REPORT] Org-share breakdown:")
display(df_reports.groupby("SharedToOrg")["ReportId"].nunique().reset_index(name="UniqueReports"))

# Dashboards active vs read-only
print("\n[DASHBOARD] Status breakdown:")
display(df_dashboards.groupby("Status")["DashboardId"].nunique().reset_index(name="UniqueDashboards"))

# Unique developers
print("\n[REPORT] Developers (report owners):")
display(df_reports[["Developer", "ReportName"]].drop_duplicates().sort_values("Developer"))

# Dashboard developers
print("\n[DASHBOARD] Active developers:")
dev_dash = df_dashboards[df_dashboards["IsDeveloper"] == "Yes"][["UserDisplayName","UserEmail","DashboardName","UserRole"]].drop_duplicates()
display(dev_dash)

print("\n✅ Audit complete.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

headers = {
    "Authorization": f"Bearer {token}"
}

workspace_id = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"

reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"

reports = requests.get(reports_url, headers=headers).json()["value"]
datasets = requests.get(datasets_url, headers=headers).json()["value"]

dataset_map = {d["id"]: d.get("configuredBy") for d in datasets}

rows = []
for r in reports:
    rows.append({
        "WorkspaceId": workspace_id
        , "ReportName": r["name"]
        , "ReportId": r["id"]
        , "DatasetId": r["datasetId"]
        , "Developer": dataset_map.get(r["datasetId"], "Unknown")
    })

df = spark.createDataFrame(pd.DataFrame(rows))
df.write.mode("overwrite").option("header", True).csv("Files/ICICI_Report_Metadata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# ICICI Bank - Power BI / Fabric Report Metadata Extractor
# Method 2: Microsoft Fabric PySpark Notebook
# Workspace: ICICI_DSAG
# Output   : Lakehouse CSV + DataFrame display
# =============================================================================
#
# PRE-REQUISITES:
#   1. This notebook must run inside Microsoft Fabric (not standalone Spark)
#   2. Attach this notebook to a Lakehouse
#   3. The Fabric workspace identity (MSI) or your user account must have:
#      - Power BI Admin role  (for /admin/* APIs)  — OR —
#      - Workspace Contributor role (for non-admin APIs)
#   4. Enable "Service Principal can use Fabric APIs" in Fabric Admin Portal
#      if using Service Principal authentication
#
# HOW TO RUN:
#   - Upload this .py file as a new Fabric Notebook
#   - Set WORKSPACE_NAME in Cell 1
#   - Run All Cells
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — CONFIGURATION  (Edit these values)
# ─────────────────────────────────────────────────────────────────────────────

WORKSPACE_NAME = "Batch_workspace"

# Output paths in Lakehouse Files section
REPORT_OUTPUT_CSV    = "Files/icici_dsag_report_audit.csv"
DASHBOARD_OUTPUT_CSV = "Files/icici_dsag_dashboard_audit.csv"

# Authentication mode: "msi" = Fabric Managed Identity (recommended in Fabric)
#                      "sp"  = Service Principal (set SP_* vars below)
#                      "pat" = Personal Access Token (dev/testing only)
AUTH_MODE = "msi"

# Service Principal credentials (only if AUTH_MODE = "sp")
SP_TENANT_ID     = ""
SP_CLIENT_ID     = ""
SP_CLIENT_SECRET = ""

# Personal Access Token (only if AUTH_MODE = "pat" — NOT for production)
PAT_TOKEN = ""


# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — IMPORTS & HELPER SETUP
# ─────────────────────────────────────────────────────────────────────────────

import requests
import json
import pandas as pd
from datetime import datetime

# Fabric / Synapse token helper
try:
    from notebookutils import mssparkutils
    IN_FABRIC = True
except ImportError:
    IN_FABRIC = False
    print("[WARN] notebookutils not found — assuming local/testing mode")

POWERBI_BASE     = "https://api.powerbi.com/v1.0/myorg"
POWERBI_ADMIN    = "https://api.powerbi.com/v1.0/myorg/admin"
RESOURCE_URL     = "https://analysis.windows.net/powerbi/api"


def get_access_token():
    """Obtain Bearer token based on AUTH_MODE."""
    if AUTH_MODE == "msi" and IN_FABRIC:
        # Use Fabric built-in MSI token provider
        token = mssparkutils.credentials.getToken(RESOURCE_URL)
        return token

    elif AUTH_MODE == "sp":
        url = f"https://login.microsoftonline.com/{SP_TENANT_ID}/oauth2/v2.0/token"
        data = {
            "grant_type"   : "client_credentials",
            "client_id"    : SP_CLIENT_ID,
            "client_secret": SP_CLIENT_SECRET,
            "scope"        : f"{RESOURCE_URL}/.default"
        }
        resp = requests.post(url, data=data)
        resp.raise_for_status()
        return resp.json()["access_token"]

    elif AUTH_MODE == "pat":
        return PAT_TOKEN

    else:
        raise ValueError(f"Unknown AUTH_MODE: {AUTH_MODE}")


def make_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type" : "application/json"
    }


def safe_get(url, headers, params=None):
    """GET request with basic error handling; returns parsed JSON or None."""
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        print(f"  [WARN] HTTP {e.response.status_code} for {url}: {e.response.text[:200]}")
        return None
    except Exception as e:
        print(f"  [WARN] Request failed for {url}: {e}")
        return None


print("[SETUP] Libraries loaded successfully.")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — AUTHENTICATE
# ─────────────────────────────────────────────────────────────────────────────

print(f"[AUTH] Authenticating using mode: {AUTH_MODE} ...")
TOKEN   = get_access_token()
HEADERS = make_headers(TOKEN)
print("[AUTH] ✅ Authentication successful.\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — RESOLVE WORKSPACE
# ─────────────────────────────────────────────────────────────────────────────

print(f"[INFO] Resolving workspace: '{WORKSPACE_NAME}' ...")

# Try regular API first
ws_resp = safe_get(
    f"{POWERBI_BASE}/groups",
    HEADERS,
    params={"$filter": f"name eq '{WORKSPACE_NAME}'", "$top": 5}
)

workspace = None
if ws_resp and ws_resp.get("value"):
    workspace = ws_resp["value"][0]
else:
    # Fallback: Admin API (requires Power BI Admin role)
    ws_resp = safe_get(
        f"{POWERBI_ADMIN}/groups",
        HEADERS,
        params={"$filter": f"name eq '{WORKSPACE_NAME}'", "$top": 5}
    )
    if ws_resp and ws_resp.get("value"):
        workspace = ws_resp["value"][0]

if not workspace:
    raise RuntimeError(
        f"Workspace '{WORKSPACE_NAME}' not found. "
        "Check name and permissions."
    )

WORKSPACE_ID   = workspace["id"]
IS_DEDICATED   = workspace.get("isOnDedicatedCapacity", False)
WORKSPACE_TYPE = workspace.get("type", "Workspace")

print(f"[INFO] ✅ Found workspace: {WORKSPACE_NAME}")
print(f"       ID             : {WORKSPACE_ID}")
print(f"       Type           : {WORKSPACE_TYPE}")
print(f"       Dedicated Cap. : {IS_DEDICATED}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — FETCH DATASETS (for developer/owner info)
# ─────────────────────────────────────────────────────────────────────────────

print("[INFO] Fetching datasets ...")
ds_resp = safe_get(f"{POWERBI_BASE}/groups/{WORKSPACE_ID}/datasets", HEADERS)
datasets_raw = ds_resp.get("value", []) if ds_resp else []

dataset_map = {}
for ds in datasets_raw:
    dataset_map[ds["id"]] = {
        "name"        : ds.get("name", "N/A"),
        "configuredBy": ds.get("configuredBy", ds.get("createdBy", "N/A")),
        "createdDate" : ds.get("createdDate", "N/A"),
        "isRefreshable": ds.get("isRefreshable", False)
    }
print(f"[INFO] ✅ {len(dataset_map)} dataset(s) found.\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — FETCH WORKSPACE MEMBERS
# ─────────────────────────────────────────────────────────────────────────────

print("[INFO] Fetching workspace members ...")
mem_resp = safe_get(f"{POWERBI_BASE}/groups/{WORKSPACE_ID}/users", HEADERS)
members_raw = mem_resp.get("value", []) if mem_resp else []

# Build email -> {role, displayName} map
member_map = {}
for m in members_raw:
    email = m.get("emailAddress") or m.get("identifier", "")
    member_map[email.lower()] = {
        "role"       : m.get("groupUserAccessRight", "Viewer"),
        "displayName": m.get("displayName", email)
    }
print(f"[INFO] ✅ {len(member_map)} workspace member(s).\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — FETCH REPORTS
# ─────────────────────────────────────────────────────────────────────────────

print("[INFO] Fetching reports ...")
rpt_resp = safe_get(f"{POWERBI_BASE}/groups/{WORKSPACE_ID}/reports", HEADERS)
reports_raw = rpt_resp.get("value", []) if rpt_resp else []
print(f"[INFO] ✅ {len(reports_raw)} report(s) found.\n")


# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — DETERMINE ORG-SHARE STATUS
# ─────────────────────────────────────────────────────────────────────────────

def get_org_share_status(report_id):
    """
    Checks if a report is published as a Power BI App visible to the entire org.
    Returns: "Yes - Org App", "No", or "Unknown"
    """
    # Check Publish-to-Web embeds (org-level public access)
    embed_resp = safe_get(
        f"https://api.powerbi.com/v1.0/myorg/admin/reports/{report_id}/datasources",
        HEADERS
    )
    # Proxy: if workspace is on dedicated capacity and has an org app, mark as possibly shared
    if IS_DEDICATED:
        return "Possibly Yes (Dedicated Capacity Workspace)"
    return "No (Standard Workspace)"


# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — BUILD REPORT AUDIT DATAFRAME
# ─────────────────────────────────────────────────────────────────────────────

print("[INFO] Building report audit rows ...")
report_rows = []

for report in reports_raw:
    report_id   = report.get("id", "N/A")
    report_name = report.get("name", "N/A")
    dataset_id  = report.get("datasetId", "N/A")
    report_type = report.get("reportType", "PowerBIReport")
    web_url     = report.get("webUrl", "N/A")
    embed_url   = report.get("embedUrl", "N/A")
    created_dt  = report.get("createdDateTime", "N/A")
    modified_dt = report.get("modifiedDateTime", "N/A")
    modified_by = report.get("modifiedBy", "N/A")

    # Developer from dataset
    ds_info   = dataset_map.get(dataset_id, {})
    developer = ds_info.get("configuredBy", "N/A")

    # Org-share status
    shared_to_org = get_org_share_status(report_id)

    # Per-report users (Admin API)
    users = []
    user_resp = safe_get(
        f"{POWERBI_ADMIN}/reports/{report_id}/users",
        HEADERS
    )
    if user_resp and user_resp.get("value"):
        users = user_resp["value"]
    else:
        # Fallback to workspace members
        users = [
            {
                "emailAddress"          : email,
                "displayName"           : info["displayName"],
                "reportUserAccessRight" : info["role"]
            }
            for email, info in member_map.items()
        ]

    if users:
        for user in users:
            email        = user.get("emailAddress") or user.get("identifier", "N/A")
            display_name = user.get("displayName", email)
            role         = (
                user.get("reportUserAccessRight")
                or member_map.get(email.lower(), {}).get("role", "Viewer")
            )
            report_rows.append({
                "WorkspaceName"   : WORKSPACE_NAME,
                "WorkspaceId"     : WORKSPACE_ID,
                "ReportId"        : report_id,
                "ReportName"      : report_name,
                "DatasetId"       : dataset_id,
                "DatasetName"     : ds_info.get("name", "N/A"),
                "Developer"       : developer,
                "UserEmail"       : email,
                "UserDisplayName" : display_name,
                "UserRole"        : role,
                "SharedToOrg"     : shared_to_org,
                "ReportType"      : report_type,
                "WebUrl"          : web_url,
                "EmbedUrl"        : embed_url,
                "CreatedDateTime" : created_dt,
                "ModifiedDateTime": modified_dt,
                "ModifiedBy"      : modified_by,
                "AuditTimestamp"  : datetime.utcnow().isoformat()
            })
    else:
        report_rows.append({
            "WorkspaceName"   : WORKSPACE_NAME,
            "WorkspaceId"     : WORKSPACE_ID,
            "ReportId"        : report_id,
            "ReportName"      : report_name,
            "DatasetId"       : dataset_id,
            "DatasetName"     : ds_info.get("name", "N/A"),
            "Developer"       : developer,
            "UserEmail"       : "N/A",
            "UserDisplayName" : "N/A",
            "UserRole"        : "N/A",
            "SharedToOrg"     : shared_to_org,
            "ReportType"      : report_type,
            "WebUrl"          : web_url,
            "EmbedUrl"        : embed_url,
            "CreatedDateTime" : created_dt,
            "ModifiedDateTime": modified_dt,
            "ModifiedBy"      : modified_by,
            "AuditTimestamp"  : datetime.utcnow().isoformat()
        })

df_reports = pd.DataFrame(report_rows)
print(f"[INFO] ✅ Report audit: {len(df_reports)} row(s) built.")
print("\n── REPORTS PREVIEW ──────────────────────────────────────────────────────────")
display(df_reports.head(20))


# ─────────────────────────────────────────────────────────────────────────────
# CELL 11 — SAVE TO LAKEHOUSE (CSV)
# ─────────────────────────────────────────────────────────────────────────────

print("\n[INFO] Saving CSVs to Lakehouse ...")

if IN_FABRIC:
    # In Fabric, write to the attached Lakehouse using Spark
    spark_df_reports    = spark.createDataFrame(df_reports)
    

    spark_df_reports.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv(f"Files/icici_dsag_report_audit")

 

    print(f"[SUCCESS] ✅ Reports CSV    → Lakehouse: Files/icici_dsag_report_audit/")
   
else:
    # Local / testing fallback
    df_reports.to_csv("icici_dsag_report_audit.csv", index=False, encoding="utf-8-sig")



# ─────────────────────────────────────────────────────────────────────────────
# CELL 12 — SUMMARY STATISTICS
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("   ICICI DSAG WORKSPACE — AUDIT SUMMARY")
print("="*60)
print(f"  Workspace      : {WORKSPACE_NAME} ({WORKSPACE_ID})")
print(f"  Reports found  : {len(reports_raw)}")
print(f"  Dashboards     : {len(dashboards_raw)}")
print(f"  Datasets       : {len(dataset_map)}")
print(f"  Workspace Users: {len(member_map)}")
print("="*60)

# Reports by org-share status
print("\n[REPORT] Org-share breakdown:")
display(df_reports.groupby("SharedToOrg")["ReportId"].nunique().reset_index(name="UniqueReports"))

# Dashboards active vs read-only
print("\n[DASHBOARD] Status breakdown:")
display(df_dashboards.groupby("Status")["DashboardId"].nunique().reset_index(name="UniqueDashboards"))

# Unique developers
print("\n[REPORT] Developers (report owners):")
display(df_reports[["Developer", "ReportName"]].drop_duplicates().sort_values("Developer"))

# Dashboard developers
print("\n[DASHBOARD] Active developers:")
dev_dash = df_dashboards[df_dashboards["IsDeveloper"] == "Yes"][["UserDisplayName","UserEmail","DashboardName","UserRole"]].drop_duplicates()
display(dev_dash)

print("\n✅ Audit complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Power BI Report Metadata + User Access Extractor
# Microsoft Fabric Notebook (PySpark)
# ============================================================

# ----- CELL 1: Install dependencies & imports -----
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from notebookutils import mssparkutils

spark = SparkSession.builder.getOrCreate()

# ----- CELL 2: Authentication -----
# Option A: Using Fabric token (recommended - no secrets needed)
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

print("✅ Token acquired successfully")

# ----- CELL 3: Define Workspace IDs to scan -----
# You can hardcode specific workspace IDs or fetch all workspaces dynamically
WORKSPACE_IDS = [
    "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"   # Batch_workspace
    # Add more workspace IDs here
]

# ----- CELL 4: Helper Functions -----

def get_all_workspaces():
    """Fetch all workspaces the service principal/user has access to"""
    url = "https://api.powerbi.com/v1.0/myorg/groups?$top=1000"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("value", [])


def get_reports_in_workspace(workspace_id):
    """Fetch all reports in a given workspace"""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("value", [])


def get_workspace_users(workspace_id):
    """Fetch all users who are members of the workspace"""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("value", [])


def get_report_users(workspace_id, report_id):
    """
    Fetch users who have been granted DIRECT/INDIVIDUAL access to a specific report.
    This covers reports shared individually (not via workspace membership).
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/users"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    elif resp.status_code == 404:
        return []  # Report may not support user listing
    else:
        print(f"  ⚠️ Could not fetch report users for {report_id}: {resp.status_code}")
        return []


def get_dataset_info(workspace_id, dataset_id):
    """Fetch dataset details"""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    return {}

# ----- CELL 5: Main Extraction Logic -----

all_records = []

for workspace_id in WORKSPACE_IDS:
    print(f"\n🔍 Processing Workspace: {workspace_id}")
    
    # --- Get Workspace-level users ---
    workspace_users = get_workspace_users(workspace_id)
    print(f"  👥 Workspace members: {len(workspace_users)}")
    
    # Build a lookup: email -> {displayName, workspaceRole}
    ws_user_map = {}
    for wu in workspace_users:
        email = wu.get("emailAddress", wu.get("identifier", "")).lower()
        ws_user_map[email] = {
            "UserDisplayName": wu.get("displayName", ""),
            "UserEmail": email,
            "UserRole": wu.get("groupUserAccessRight", ""),
            "AccessType": "WorkspaceMember",
            "PrincipalType": wu.get("principalType", "User")
        }
    
    # --- Get Reports ---
    reports = get_reports_in_workspace(workspace_id)
    print(f"  📊 Reports found: {len(reports)}")
    
    for report in reports:
        report_id = report.get("id")
        report_name = report.get("name")
        dataset_id = report.get("datasetId", "")
        report_type = report.get("reportType", "PowerBIReport")
        web_url = report.get("webUrl", "")
        embed_url = report.get("embedUrl", "")
        
        print(f"\n  📄 Report: {report_name} ({report_id})")
        
        # --- Get Dataset info ---
        dataset_info = get_dataset_info(workspace_id, dataset_id) if dataset_id else {}
        dataset_name = dataset_info.get("name", "")
        developer = dataset_info.get("configuredBy", "")
        
        # --- Get Report-level (individually shared) users ---
        report_users = get_report_users(workspace_id, report_id)
        print(f"    🔑 Direct report shares: {len(report_users)}")
        
        # Build report-level user map
        rpt_user_map = {}
        for ru in report_users:
            email = ru.get("emailAddress", ru.get("identifier", "")).lower()
            rpt_user_map[email] = {
                "UserDisplayName": ru.get("displayName", ""),
                "UserEmail": email,
                "UserRole": ru.get("reportUserAccessRight", ""),
                "AccessType": "DirectReportShare",
                "PrincipalType": ru.get("principalType", "User")
            }
        
        # --- Merge: workspace users + report-direct users ---
        # Start with workspace users
        merged_users = {}
        for email, info in ws_user_map.items():
            merged_users[email] = {**info}
        
        # Overlay/add report-direct users
        for email, info in rpt_user_map.items():
            if email in merged_users:
                # User exists at workspace level AND has direct share → mark both
                merged_users[email]["AccessType"] = "WorkspaceMember + DirectReportShare"
                merged_users[email]["ReportShareRole"] = info["UserRole"]
            else:
                # User only has direct report share (individual share outside workspace)
                merged_users[email] = {**info}
        
        # --- Build final records ---
        for email, user_info in merged_users.items():
            all_records.append({
                "WorkspaceName": "Batch_workspace",   # Fetch from workspace metadata if needed
                "WorkspaceId": workspace_id,
                "ReportId": report_id,
                "ReportName": report_name,
                "DatasetId": dataset_id,
                "DatasetName": dataset_name,
                "Developer": developer,
                "ReportType": report_type,
                "WebUrl": web_url,
                "EmbedUrl": embed_url,
                "UserEmail": user_info.get("UserEmail", ""),
                "UserDisplayName": user_info.get("UserDisplayName", ""),
                "UserRole": user_info.get("UserRole", ""),
                "AccessType": user_info.get("AccessType", ""),
                "ReportShareRole": user_info.get("ReportShareRole", ""),
                "PrincipalType": user_info.get("PrincipalType", "User")
            })

print(f"\n✅ Total records collected: {len(all_records)}")

# ----- CELL 6: Convert to Spark DataFrame -----

schema = StructType([
    StructField("WorkspaceName",     StringType(), True),
    StructField("WorkspaceId",       StringType(), True),
    StructField("ReportId",          StringType(), True),
    StructField("ReportName",        StringType(), True),
    StructField("DatasetId",         StringType(), True),
    StructField("DatasetName",       StringType(), True),
    StructField("Developer",         StringType(), True),
    StructField("ReportType",        StringType(), True),
    StructField("WebUrl",            StringType(), True),
    StructField("EmbedUrl",          StringType(), True),
    StructField("UserEmail",         StringType(), True),
    StructField("UserDisplayName",   StringType(), True),
    StructField("UserRole",          StringType(), True),
    StructField("AccessType",        StringType(), True),
    StructField("ReportShareRole",   StringType(), True),
    StructField("PrincipalType",     StringType(), True),
])

df = spark.createDataFrame(all_records, schema=schema)

df.show(truncate=False)

# ----- CELL 7: Filter - Users with Direct Report Shares only -----

print("\n📌 Users with INDIVIDUAL/DIRECT report shares:")
df.filter(col("AccessType").contains("DirectReportShare")).show(truncate=False)

# ----- CELL 8: Summary per Report -----

print("\n📊 User count per Report:")
df.groupBy("WorkspaceName", "ReportName", "AccessType") \
  .agg(count("UserEmail").alias("UserCount")) \
  .orderBy("ReportName", "AccessType") \
  .show(truncate=False)

# ----- CELL 9: Save to Lakehouse Delta Table (optional) -----

# Uncomment to persist to your Fabric Lakehouse
# df.write.format("delta") \
#     .mode("overwrite") \
#     .option("mergeSchema", "true") \
#     .saveAsTable("powerbi_report_user_access")

# print("✅ Saved to Delta table: powerbi_report_user_access")

# ----- CELL 10: Export to CSV in Lakehouse Files (optional) -----

# df.coalesce(1).write.mode("overwrite").option("header", "true") \
#     .csv("Files/powerbi_metadata/report_user_access.csv")

# print("✅ Exported to Lakehouse Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----- FINAL CELL: Display + Export Tabular Output -----

from pyspark.sql.functions import collect_set, when, concat_ws, expr

print("\n📊 Generating Tabular Output...")

# Step 1: Aggregate (1 row per report)
tabular_df = df.groupBy(
    "WorkspaceName",
    "ReportName",
    "DatasetName",
    "ReportType"
).agg(
    collect_set(
        when(col("UserRole").isin("Admin", "Member"), col("UserEmail"))
    ).alias("Developers"),

    collect_set(
        when(col("UserRole") == "Contributor", col("UserEmail"))
    ).alias("Contributors"),

    collect_set(
        when(col("UserRole") == "Viewer", col("UserEmail"))
    ).alias("Viewers")
)

# Step 2: Remove nulls
tabular_df = tabular_df.select(
    "WorkspaceName",
    "ReportName",
    "DatasetName",
    "ReportType",
    expr("filter(Developers, x -> x is not null)").alias("Developers"),
    expr("filter(Contributors, x -> x is not null)").alias("Contributors"),
    expr("filter(Viewers, x -> x is not null)").alias("Viewers")
)

# Step 3: Convert arrays → comma-separated
tabular_df = tabular_df.select(
    "WorkspaceName",
    "ReportName",
    "DatasetName",
    "ReportType",
    concat_ws(", ", "Developers").alias("Developers"),
    concat_ws(", ", "Contributors").alias("Contributors"),
    concat_ws(", ", "Viewers").alias("Viewers")
)

# 🔹 DISPLAY OUTPUT (use show for PySpark)
print("\n📋 Tabular View:")
tabular_df.orderBy("WorkspaceName", "ReportName").show(truncate=False)

# (Optional UI display if supported in Fabric)
try:
    display(tabular_df)
except:
    pass

# 🔹 EXPORT TO CSV
output_path = "Files/powerbi_metadata/report_summary_tabular"

tabular_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"\n✅ CSV exported successfully to: {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
