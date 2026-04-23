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

import requests
import pandas as pd

# Auto token from Fabric
from notebookutils import mssparkutils
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
pd.set_option("display.max_columns", None)  
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
        "workspace_name": "Batch_workspace",
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

print(f"Total rows in DataFrame: {spark_df.count()}")

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

# Auto token from Fabric
from notebookutils import mssparkutils
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

pd.set_option("display.max_columns", None)

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
        "workspace_name": "Batch_workspace",
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
# STEP 4: CONVERT TO SPARK
# =========================
spark_df = spark.createDataFrame(final_df)

print(f"Total rows in DataFrame: {spark_df.count()}")

# =========================
# STEP 5: ADD ROW NUMBER
# =========================
window_spec = Window.orderBy("report_id", "user_email")

df_with_row = spark_df.withColumn("row_num", row_number().over(window_spec))

# =========================
# STEP 6: SPLIT INTO 4 DATAFRAMES (10K EACH)
# =========================
df1 = df_with_row.filter("row_num <= 10")
df2 = df_with_row.filter("row_num > 10 AND row_num <= 20")
df3 = df_with_row.filter("row_num > 20 AND row_num <= 30")
df4 = df_with_row.filter("row_num > 30 AND row_num <= 40")

# =========================
# STEP 7: DROP ROW NUMBER (OPTIONAL)
# =========================
#df1 = df1.drop("row_num")
#df2 = df2.drop("row_num")
#df3 = df3.drop("row_num")
#df4 = df4.drop("row_num")

# =========================
# STEP 8: DISPLAY
# =========================
display(df1)
display(df2)
display(df3)
display(df4)
display(df_with_row)
# =========================
# STEP 9: SAVE TO LAKEHOUSE
# =========================
df1.write.mode("overwrite").format("csv").option("header", "true").save("Files/output_part1")
df2.write.mode("overwrite").format("csv").option("header", "true").save("Files/output_part2")
df3.write.mode("overwrite").format("csv").option("header", "true").save("Files/output_part3")
df4.write.mode("overwrite").format("csv").option("header", "true").save("Files/output_part4")

# =========================
# OPTIONAL: SAVE FULL TABLE ALSO
# =========================
spark_df.write.mode("overwrite").saveAsTable("final_governance_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM final_governance_table")

# ✅ Correct - no leading slash before Files
df.coalesce(1).write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv("Files/powerbi_users_export/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if folder and file were created
files = mssparkutils.fs.ls("/lakehouse/default/Files/powerbi_users_final.csv")
for f in files:
    print(f.name, f.size)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the CSV back and convert to pandas
df = spark.sql("SELECT * FROM final_governance_table")

# Convert to pandas and save as Excel
pdf = df.toPandas()

# Save to the ROOT of Files (not inside a subfolder)
pdf.to_csv("/lakehouse/default/Files/powerbi_users_final.csv", index=False)

print(f"✅ Total rows exported: {len(pdf)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Window
from pyspark.sql.functions import ntile, col

df = spark.sql("SELECT * FROM final_governance_table")
total = df.count()
print(f"Total records: {total:,}")

# Split into 4 buckets of ~10K each
df_bucketed = df.withColumn("bucket", ntile(4).over(Window.orderBy("workspace_name")))

# Export each bucket as separate CSV at Files ROOT level
for i in range(1, 5):
    bucket_df = df_bucketed.filter(col("bucket") == i).drop("bucket")
    
    # Convert to pandas and save directly to Files root
    pdf = bucket_df.toPandas()
    pdf.to_csv(f"/lakehouse/default/Files/Lpowerbi_users_part{i}.csv", index=False)
    print(f"✅ Part {i}: {len(pdf):,} rows saved → powerbi_users_part{i}.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/org_wide_shared_reports/part-00000-4788c1d2-a575-461d-83da-0379fb425c1c-c000.csv")
# df now is a Spark DataFrame containing CSV data from "Files/org_wide_shared_reports/part-00000-4788c1d2-a575-461d-83da-0379fb425c1c-c000.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/powerbi_users_export/part-00000-518fee01-bee3-4bb8-938f-107a1cb8195e-c000.csv")
# df now is a Spark DataFrame containing CSV data from "Files/powerbi_users_export/part-00000-518fee01-bee3-4bb8-938f-107a1cb8195e-c000.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df.groupBy("role").count().display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) from final_governance_table

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# ----- CELL: Call Power BI Admin API (Widely Shared Artifacts) -----

import requests
from notebookutils import mssparkutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# 🔐 Get Fabric token (no need to paste token manually)
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# API URL
url = "https://api.powerbi.com/v1.0/myorg/admin/widelySharedArtifacts/linksSharedToWholeOrganization"

print("🔍 Calling Power BI Admin API...")

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json().get("value", [])
    print(f"✅ Records fetched: {len(data)}")
else:
    print(f"❌ Error: {response.status_code} - {response.text}")
    data = []

# Convert to Spark DataFrame
if data:
    df_shared = spark.createDataFrame(data)
    
    print("\n📊 Preview:")
    df_shared.show(truncate=False)
else:
    print("⚠️ No data found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Power BI - Org Wide Shareable Links استخراج (Fabric Notebook)
# API: /admin/widelySharedArtifacts/linksSharedToWholeOrganization
# ============================================================

import requests
import pandas as pd
from pyspark.sql import SparkSession
from notebookutils import mssparkutils

spark = SparkSession.builder.getOrCreate()
pd.set_option("display.max_columns", None) 
# ------------------------------------------------------------
# STEP 1: AUTHENTICATION (Fabric Token)
# ------------------------------------------------------------
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

print("✅ Token acquired")

# ------------------------------------------------------------
# STEP 2: CALL ADMIN API
# ------------------------------------------------------------
url = "https://api.powerbi.com/v1.0/myorg/admin/widelySharedArtifacts/linksSharedToWholeOrganization"

print("🔍 Calling Power BI Admin API...")

response = requests.get(url, headers=headers)

if response.status_code != 200:
    print(f"❌ API Failed: {response.status_code}")
    print(response.text)
    raise Exception("API call failed")

data = response.json()

records = data.get("ArtifactAccessEntities", [])

print(f"✅ Records fetched: {len(records)}")

# ------------------------------------------------------------
# STEP 3: TRANSFORM DATA → TABULAR
# ------------------------------------------------------------
rows = []

for item in records:
    sharer = item.get("sharer", {})

    rows.append({
        "ArtifactId"     : item.get("artifactId"),
        "ReportName"     : item.get("displayName"),
        "ArtifactType"   : item.get("artifactType"),
        "AccessRight"    : item.get("accessRight"),
        "ShareType"      : item.get("shareType"),
        "SharerName"     : sharer.get("displayName"),
        "SharerEmail"    : sharer.get("emailAddress"),
        "PrincipalType"  : sharer.get("principalType"),
        
        # 👇 Map to business-friendly role
        "RoleMapped" : (
            "Viewer" if item.get("accessRight") == "Read"
            else "Contributor" if item.get("accessRight") == "ReadReshare"
            else "Member"
        )
    })

# Convert to Pandas
df_pd = pd.DataFrame(rows)

# ------------------------------------------------------------
# STEP 4: DISPLAY (TABULAR FORMAT)
# ------------------------------------------------------------
print("\n📊 Preview:")
display(df_pd)

# ------------------------------------------------------------
# STEP 5: CONVERT TO SPARK DATAFRAME
# ------------------------------------------------------------
df_spark = spark.createDataFrame(df_pd)

# Show in Spark format
df_spark.show(truncate=False)

print(f"Total rows in DataFrame: {df_spark.count()}")

df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("org_wide_shared_reports")

print(f"\n✅ Table saved: org_wide_shared_reports")

# ------------------------------------------------------------
# STEP 6: SAVE AS CSV (Lakehouse)
# ------------------------------------------------------------
output_path = "Files/org_wide_shared_reports"

df_spark.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"\n✅ CSV saved at: {output_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Power BI - Org Wide Shareable Links (with Workspace Name)
# ============================================================

import requests
import pandas as pd
from pyspark.sql import SparkSession
from notebookutils import mssparkutils

spark = SparkSession.builder.getOrCreate()
pd.set_option("display.max_columns", None)

# ------------------------------------------------------------
# STEP 1: AUTHENTICATION
# ------------------------------------------------------------
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

print("✅ Token acquired")

# ------------------------------------------------------------
# STEP 2: BUILD REPORT → WORKSPACE MAPPING
# ------------------------------------------------------------
def get_report_workspace_map():
    mapping = {}

    print("🔍 Fetching workspaces...")

    ws_url = "https://api.powerbi.com/v1.0/myorg/groups?$top=5000"
    ws_resp = requests.get(ws_url, headers=headers)

    if ws_resp.status_code != 200:
        print("❌ Failed to fetch workspaces")
        return mapping

    workspaces = ws_resp.json().get("value", [])
    print(f"✅ Workspaces found: {len(workspaces)}")

    for ws in workspaces:
        ws_id = ws.get("id")
        ws_name = ws.get("name")

        rpt_url = f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/reports"
        rpt_resp = requests.get(rpt_url, headers=headers)

        if rpt_resp.status_code != 200:
            continue

        reports = rpt_resp.json().get("value", [])

        for rpt in reports:
            mapping[rpt.get("id")] = ws_name

    print(f"✅ Mapping built for {len(mapping)} reports\n")
    return mapping


report_workspace_map = get_report_workspace_map()

# ------------------------------------------------------------
# STEP 3: CALL ADMIN API
# ------------------------------------------------------------
url = "https://api.powerbi.com/v1.0/myorg/admin/widelySharedArtifacts/linksSharedToWholeOrganization"

print("🔍 Calling Power BI Admin API...")

response = requests.get(url, headers=headers)

if response.status_code != 200:
    print(f"❌ API Failed: {response.status_code}")
    print(response.text)
    raise Exception("API call failed")

data = response.json()
records = data.get("ArtifactAccessEntities", [])

print(f"✅ Records fetched: {len(records)}")

# ------------------------------------------------------------
# STEP 4: TRANSFORM DATA → TABULAR
# ------------------------------------------------------------
rows = []

for item in records:
    sharer = item.get("sharer", {})
    report_id = item.get("artifactId")

    rows.append({
        "WorkspaceName" : report_workspace_map.get(report_id, "Unknown"),  # ✅ NEW

        "ArtifactId"     : report_id,
        "ReportName"     : item.get("displayName"),
        "ArtifactType"   : item.get("artifactType"),
        "AccessRight"    : item.get("accessRight"),
        "ShareType"      : item.get("shareType"),
        "SharerName"     : sharer.get("displayName"),
        "SharerEmail"    : sharer.get("emailAddress"),
        "PrincipalType"  : sharer.get("principalType"),

        "RoleMapped" : (
            "Viewer" if item.get("accessRight") == "Read"
            else "Contributor" if item.get("accessRight") == "ReadReshare"
            else "Member"
        )
    })

# Convert to Pandas
df_pd = pd.DataFrame(rows)

# ------------------------------------------------------------
# STEP 5: DISPLAY
# ------------------------------------------------------------
print("\n📊 Preview:")
display(df_pd)

# ------------------------------------------------------------
# STEP 6: CONVERT TO SPARK
# ------------------------------------------------------------
df_spark = spark.createDataFrame(df_pd)

df_spark.show(truncate=False)
print(f"Total rows in DataFrame: {df_spark.count()}")

# ------------------------------------------------------------
# STEP 7: SAVE AS TABLE
# ------------------------------------------------------------
df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("org_wide_shared_reports")

print(f"\n✅ Table saved: org_wide_shared_reports")

# ------------------------------------------------------------
# STEP 8: SAVE AS CSV
# ------------------------------------------------------------
output_path = "Files/org_wide_shared_reports"

df_spark.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"\n✅ CSV saved at: {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**Get active reports and users**</mark>

# CELL ********************

import requests
import pandas as pd
from notebookutils import mssparkutils

# ----------------------------
# CONFIG
# ----------------------------
workspace_name = "Batch_workspace"
workspace_id   = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"  # <-- change as needed

# ----------------------------
# AUTH (Fabric-managed token)
# ----------------------------
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
headers = {"Authorization": f"Bearer {token}"}

# ----------------------------
# STEP 1: Get reports in the workspace
# ----------------------------
reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
reports = requests.get(reports_url, headers=headers).json().get("value", [])

rows = []
for r in reports:
    report_id  = r.get("id")
    report_name = r.get("name")
    dataset_id = r.get("datasetId")

    owner = None
    if dataset_id:
        # NOTE: Same approach you used earlier: dataset configuredBy as owner/developer 【1-0dc14b】
        dataset_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
        ds = requests.get(dataset_url, headers=headers).json()
        owner = ds.get("configuredBy")

    rows.append({
        "WorkspaceName": workspace_name,
        "WorkspaceId": workspace_id,
        "ReportName": report_name,
        "ReportId": report_id,
        "DatasetId": dataset_id,
        "Owner": owner
    })

# ----------------------------
# STEP 2: Convert to Spark DF and display
# ----------------------------
df_pd = pd.DataFrame(rows)
spark_df = spark.createDataFrame(df_pd)

display(spark_df)
print(f"Total rows: {spark_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**Get Organization wide shared reports
# **</mark>

# CELL ********************

import requests
import pandas as pd
from notebookutils import mssparkutils

# ============================================================
# CONFIG
# ============================================================
BASE = "https://api.powerbi.com/v1.0/myorg"
ADMIN_ENDPOINT = f"{BASE}/admin/widelySharedArtifacts/linksSharedToWholeOrganization"

# ============================================================
# AUTH (Fabric-managed token)
# ============================================================
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ============================================================
# Helper: GET with pagination
# - Admin API returns continuationUri/continuationToken (paging) 【3-0173d4】
# - Some OData endpoints return @odata.nextLink (paging)
# ============================================================
def paged_get(url, headers, mode="auto"):
    """
    Yields items from paged responses.
    Handles:
      - Admin continuationUri
      - OData @odata.nextLink
    """
    next_url = url
    while next_url:
        resp = requests.get(next_url, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        # 1) WidelySharedArtifacts uses artifactAccessEntities + continuationUri 【3-0173d4】
        if "artifactAccessEntities" in data or "ArtifactAccessEntities" in data:
            items = data.get("artifactAccessEntities") or data.get("ArtifactAccessEntities") or []
            for it in items:
                yield it
            next_url = data.get("continuationUri")  # full URL

        # 2) Standard OData style uses value + @odata.nextLink
        elif "value" in data:
            for it in data.get("value", []):
                yield it
            next_url = data.get("@odata.nextLink")

        else:
            # No known paging structure; stop
            next_url = None

# ============================================================
# STEP 1: Build ReportId -> WorkspaceName mapping
#   - List ALL workspaces (groups)
#   - For each workspace, list reports and map reportId -> workspaceName
#   (same approach you used in earlier Fabric notebook code patterns) 【2-12a47a】
# ============================================================
report_to_wsname = {}

# Workspaces (groups) - request top=5000 per page (then page if nextLink exists)
groups_url = f"{BASE}/groups?$top=5000"

workspace_count = 0
report_count = 0

for ws in paged_get(groups_url, headers):
    ws_id = ws.get("id")
    ws_name = ws.get("name")
    if not ws_id:
        continue

    workspace_count += 1

    # Reports inside workspace
    reports_url = f"{BASE}/groups/{ws_id}/reports"
    try:
        for rpt in paged_get(reports_url, headers):
            rid = rpt.get("id")
            if rid:
                report_to_wsname[rid] = ws_name
                report_count += 1
    except Exception:
        # If access is denied to some workspaces, skip them (similar to try/catch in PS)
        continue

print(f"Workspaces scanned: {workspace_count}")
print(f"Reports mapped: {report_count}")

# ============================================================
# STEP 2: Call Admin API and build final rows
# ============================================================
rows = []

for item in paged_get(ADMIN_ENDPOINT, headers):
    # Admin response fields include artifactId/displayName/artifactType/accessRight/shareType/sharer 【3-0173d4】
    if item.get("artifactType") != "Report":
        continue

    report_id = item.get("artifactId")
    report_name = item.get("displayName")

    rows.append({
        "WorkspaceName": report_to_wsname.get(report_id, "Unknown"),
        "ReportName": report_name,
        "ReportId": report_id,
        "SharingScope": "Shared with Entire Organization (Link)"
    })

# ============================================================
# STEP 3: Create Spark DataFrame and display
# ============================================================
df_pd = pd.DataFrame(rows)
spark_df = spark.createDataFrame(df_pd)

display(spark_df)
print(f"Org-wide shared report link rows: {spark_df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**Individual sharing reports**</mark>

# CELL ********************

import requests
import pandas as pd
from notebookutils import mssparkutils

# ----------------------------
# CONFIG
# ----------------------------
workspace_name = "Batch_workspace"
workspace_id   = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"   # <-- Batch_workspace id (change if needed)

BASE = "https://api.powerbi.com/v1.0/myorg"

# ----------------------------
# AUTH (Fabric token)
# ----------------------------
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ----------------------------
# STEP 1: Get all reports in the workspace
# ----------------------------
reports_url = f"{BASE}/groups/{workspace_id}/reports"
reports = requests.get(reports_url, headers=headers).json().get("value", [])

rows = []

# ----------------------------
# STEP 2: For each report, call Admin API to get users
# Admin API: GET /admin/reports/{reportId}/users  【1-9dd63f】
# ----------------------------
for r in reports:
    report_id = r.get("id")
    report_name = r.get("name")

    if not report_id:
        continue

    users_url = f"{BASE}/admin/reports/{report_id}/users"
    resp = requests.get(users_url, headers=headers)
    resp.raise_for_status()
    users = resp.json().get("value", [])

    for u in users:
        shared_to = u.get("emailAddress") or u.get("identifier") or u.get("displayName")

        # principalType can be "None" for whole-org level access in admin APIs 【1-9dd63f】
        if u.get("principalType") == "None":
            shared_to = "Entire Organization"

        rows.append({
            "WorkspaceName": workspace_name,
            "ReportName": report_name,
            "ReportId": report_id,
            "SharedTo": shared_to,
            "PrincipalType": u.get("principalType"),
            "Role": u.get("reportUserAccessRight")
        })

# ----------------------------
# STEP 3: Create Spark DataFrame + display
# ----------------------------
df_pd = pd.DataFrame(rows)
spark_df = spark.createDataFrame(df_pd)

display(spark_df)
#spark_df.distinct().display()
#spark_df.dropduplicates().show()
display(spark_df.distinct())
display(spark_df.select("WorkspaceName", "ReportName").distinct())
print("Total rows:", spark_df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
from notebookutils import mssparkutils

# ========== CONFIG ==========
WORKSPACE_ID = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"   # <-- your workspace id
FABRIC_SCOPE = "https://api.fabric.microsoft.com"

# ========== AUTH ==========
token = mssparkutils.credentials.getToken(FABRIC_SCOPE)
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ========== HELPER: PAGINATION (if API returns continuation) ==========
def get_all_pages(url):
    all_items = []
    while url:
        resp = requests.get(url, headers=headers)
        if resp.status_code >= 300:
            raise Exception(f"HTTP {resp.status_code} - {resp.text}")
        data = resp.json()

        # Most Fabric APIs use "value" arrays
        items = data.get("value", [])
        all_items.extend(items)

        # Pagination patterns (handle both)
        url = data.get("continuationUri") or data.get("@odata.nextLink")

    return all_items

# ========== CALL FABRIC ROLE ASSIGNMENTS ==========
url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/roleAssignments"
role_assignments = get_all_pages(url)

print(f"Total role assignment records: {len(role_assignments)}")

# ========== NORMALIZE ==========
rows = []
for ra in role_assignments:
    # Fabric payloads commonly look like: principal + role
    principal = ra.get("principal", {}) or {}

    rows.append({
        "workspace_id": WORKSPACE_ID,
        "principal_id": principal.get("id"),
        "principal_type": principal.get("type"),     # User / Group / ServicePrincipal (depends on tenant)
        "principal_display_name": principal.get("displayName"),
        "principal_email": principal.get("email"),   # may be null for groups/SPs
        "role": ra.get("role")                       # Admin / Member / Contributor / Viewer
    })

df_pd = pd.DataFrame(rows)

# Show in notebook
display(df_pd)

# Convert to Spark DF for saving
df_spark = spark.createDataFrame(df_pd)
display(df_spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# FABRIC NOTEBOOK SCRIPT: WORKSPACE USERS + ROLES (WITH EMAIL/UPN)
# Endpoint: GET /v1/workspaces/{workspaceId}/roleAssignments
# ============================================

import requests
import pandas as pd
from notebookutils import mssparkutils

# ---------- CONFIG ----------
WORKSPACE_ID = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"   # <-- change if needed
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"

# ---------- AUTH ----------
token = mssparkutils.credentials.getToken(FABRIC_RESOURCE)
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ---------- PAGED GET ----------
def get_all_pages(url: str):
    all_items = []
    while url:
        resp = requests.get(url, headers=headers)
        if resp.status_code >= 300:
            raise Exception(f"HTTP {resp.status_code} calling {url}\n{resp.text}")

        data = resp.json()
        all_items.extend(data.get("value", []))

        # Fabric APIs may return continuationUri; some return @odata.nextLink
        url = data.get("continuationUri") or data.get("@odata.nextLink")

    return all_items

# ---------- CALL API ----------
url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/roleAssignments"
role_assignments = get_all_pages(url)
print(f"Fetched role assignments: {len(role_assignments)}")

# ---------- NORMALIZE ----------
rows = []
for ra in role_assignments:
    principal = ra.get("principal", {}) or {}
    ptype = principal.get("type")
    display_name = principal.get("displayName")
    pid = principal.get("id") or ra.get("id")
    role = ra.get("role")

    # user email/UPN is under principal.userDetails.userPrincipalName (as in your JSON)
    user_details = principal.get("userDetails", {}) or {}
    group_details = principal.get("groupDetails", {}) or {}
    sp_details = principal.get("servicePrincipalDetails", {}) or {}

    email_or_upn = None
    if ptype == "User":
        email_or_upn = user_details.get("userPrincipalName")
    elif ptype == "Group":
        # groups often don't provide email here; keep best-effort
        email_or_upn = group_details.get("email") or group_details.get("groupEmail")
    elif ptype in ("ServicePrincipal", "App"):
        # best-effort identifier for apps
        email_or_upn = sp_details.get("appId") or sp_details.get("displayName")

    rows.append({
        "workspace_id": WORKSPACE_ID,
        "principal_id": pid,
        "principal_type": ptype,
        "principal_display_name": display_name,
        "principal_email_or_upn": email_or_upn,   # ✅ this will be filled for Users
        "role": role
    })

df_pd = pd.DataFrame(rows)

# ---------- DISPLAY ----------
display(df_pd)

df_spark = spark.createDataFrame(df_pd)
display(df_spark)

# ---------- OPTIONAL: SAVE AS LAKEHOUSE TABLE ----------
# df_spark.write.mode("overwrite").saveAsTable("workspace_role_assignments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import requests
import pandas as pd
from notebookutils import mssparkutils

# ----------------------------
# CONFIG
# ----------------------------
workspace_name = "Batch_workspace"
workspace_id   = "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f"
BASE = "https://api.powerbi.com/v1.0/myorg"

# ----------------------------
# AUTH (Fabric token for Power BI REST)
# ----------------------------
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ----------------------------
# Helper: GET with retry for 429 throttling
# ----------------------------
def get_with_retry(url, headers, max_retries=6):
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers)

        # Success
        if resp.status_code < 300:
            return resp

        # Throttling: 429
        if resp.status_code == 429:
            # Use Retry-After if provided, else exponential backoff
            retry_after = resp.headers.get("Retry-After")
            sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else (2 ** attempt)
            time.sleep(sleep_s)
            continue

        # Non-retryable
        return resp

    return resp  # last response

# ----------------------------
# STEP 0: Quick check if Admin APIs are accessible (optional, but helpful)
# ----------------------------
admin_test = get_with_retry(f"{BASE}/admin/groups?$top=1", headers)
admin_api_access = (admin_test.status_code < 300)

print("Admin API access test status:", admin_test.status_code)
if not admin_api_access:
    print("NOTE: Admin APIs not accessible for this identity. Report-level sharing will show as Unauthorized.")

# ----------------------------
# STEP 1: Get all reports in the workspace
# ----------------------------
reports_url = f"{BASE}/groups/{workspace_id}/reports"
reports_resp = get_with_retry(reports_url, headers)
reports_resp.raise_for_status()
reports = reports_resp.json().get("value", [])

rows = []

# ----------------------------
# STEP 2: For each report, call Admin API to get report users
# Endpoint: GET /admin/reports/{reportId}/users
# ----------------------------
for r in reports:
    report_id = r.get("id")
    report_name = r.get("name")
    if not report_id:
        continue

    users_url = f"{BASE}/admin/reports/{report_id}/users"
    resp = get_with_retry(users_url, headers)

    # Handle auth failures gracefully (no crash)
    if resp.status_code in (401, 403):
        rows.append({
            "WorkspaceName": workspace_name,
            "ReportName": report_name,
            "ReportId": report_id,
            "SharedTo": None,
            "PrincipalType": None,
            "Role": None,
            "Error": f"{resp.status_code} Unauthorized/Forbidden: Admin API requires Fabric/Power BI admin or approved service principal"
        })
        continue

    # Other errors also recorded (no crash)
    if resp.status_code >= 300:
        rows.append({
            "WorkspaceName": workspace_name,
            "ReportName": report_name,
            "ReportId": report_id,
            "SharedTo": None,
            "PrincipalType": None,
            "Role": None,
            "Error": f"{resp.status_code}: {resp.text[:300]}"
        })
        continue

    users = resp.json().get("value", [])

    # If no users returned, still write a row so the report appears in output
    if not users:
        rows.append({
            "WorkspaceName": workspace_name,
            "ReportName": report_name,
            "ReportId": report_id,
            "SharedTo": None,
            "PrincipalType": None,
            "Role": None,
            "Error": None
        })
        continue

    for u in users:
        shared_to = u.get("emailAddress") or u.get("identifier") or u.get("displayName")

        # "None" principalType indicates whole-org link style in admin APIs
        if u.get("principalType") == "None":
            shared_to = "Entire Organization"

        rows.append({
            "WorkspaceName": workspace_name,
            "ReportName": report_name,
            "ReportId": report_id,
            "SharedTo": shared_to,
            "PrincipalType": u.get("principalType"),
            "Role": u.get("reportUserAccessRight"),
            "Error": None
        })

# ----------------------------
# STEP 3: Create Spark DataFrame + display
# ----------------------------
df_pd = pd.DataFrame(rows)
spark_df = spark.createDataFrame(df_pd)

display(spark_df)
display(spark_df.select("WorkspaceName", "ReportName", "ReportId").distinct())

print("Total rows:", spark_df.count())
print("Rows with errors:", spark_df.filter("Error is not null").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
