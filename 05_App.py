# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Module 05 — Shareable app (prompt)
# MAGIC The complete, working Streamlit app already lives in **`solution/05_app/`** (summit banner, KPIs,
# MAGIC sentiment donut, cumulative sentiment-over-time line, AI-theme bars, full LinkedIn post wall, AI Week map,
# MAGIC and a Genie sidebar, auto-refreshing every 15s). This prompt just **deploys it** — no authoring, so it's
# MAGIC fast and reliable. Paste the prompt below into the Databricks Assistant / Genie Code.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt — copy everything below
# MAGIC ```
# MAGIC Deploy the existing Streamlit app in solution/05_app/ as a Databricks App named "ai-summit-bcn".
# MAGIC Do NOT rewrite or regenerate the app code — use the files in that folder as-is. This is a single,
# MAGIC self-contained task: do NOT create a dashboard or a Genie space.
# MAGIC
# MAGIC 1. Genie space: look up the space named "AI Summit BCN - LinkedIn" (list Genie spaces and match the
# MAGIC    title). Use its id as the app env var GENIE_SPACE_ID. Do NOT create a new space; if none with that
# MAGIC    name exists, STOP and tell me to run Module 04 first.
# MAGIC 2. Warehouse: set env var WAREHOUSE_ID to the Serverless Starter Warehouse id.
# MAGIC 3. Permissions: grant the app service principal SELECT on workspace.ai_summit, CAN_USE on that warehouse,
# MAGIC    and CAN_RUN on the "AI Summit BCN - LinkedIn" Genie space.
# MAGIC 4. If an app named "ai-summit-bcn" already exists, deploy a new version to it — do NOT create a second app.
# MAGIC 5. Deploy once, return the app URL, confirm HTTP 200 on /_stcore/health, then STOP.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fallback — grant the app's permissions yourself
# MAGIC If the Assistant deployed the app but skipped step 3, run this cell. It looks everything up by name
# MAGIC (app, warehouse, Genie space) so there are no workspace-specific ids to edit.

# COMMAND ----------

# DBTITLE 1,Grant permissions to app service principal
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import WarehouseAccessControlRequest, WarehousePermissionLevel

APP_NAME, GENIE_TITLE = "ai-summit-bcn", "AI Summit BCN - LinkedIn"
CATALOG, SCHEMA = "workspace", "ai_summit"
w = WorkspaceClient()

app = w.apps.get(APP_NAME)
sp = app.service_principal_client_id          # UC and permission APIs address service principals by application id
print("App service principal:", app.service_principal_name, "/", sp)

# 1. SELECT on the schema
for stmt in [f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{sp}`",
             f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{sp}`",
             f"GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMA} TO `{sp}`"]:
    spark.sql(stmt)
    print("Done:", stmt)

# 2. CAN_USE on the Serverless Starter Warehouse
whs = list(w.warehouses.list())
wh = (next((x for x in whs if "Serverless Starter" in (x.name or "")), None)
      or next((x for x in whs if x.enable_serverless_compute), None))
w.warehouses.update_permissions(
    warehouse_id=wh.id,
    access_control_list=[WarehouseAccessControlRequest(service_principal_name=sp,
                                                       permission_level=WarehousePermissionLevel.CAN_USE)])
print(f"Done: CAN_USE on warehouse {wh.name} ({wh.id})")

# 3. CAN_RUN on the Genie space (found by title)
spaces = w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
space_id = next((s["space_id"] for s in spaces if s.get("title") == GENIE_TITLE), None)
if not space_id:
    raise ValueError(f"No Genie space titled '{GENIE_TITLE}' — run Module 04 first.")
acl = {"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_RUN"}]}
for path in (f"/api/2.0/permissions/genie/{space_id}", f"/api/2.0/permissions/genie-spaces/{space_id}"):
    try:
        w.api_client.do("PATCH", path, body=acl)
        print(f"Done: CAN_RUN on Genie space {space_id}")
        break
    except Exception as e:
        print(f"  {path} -> {e}")

print(f"\nAll permissions granted. App URL: {app.url}")
print(f"Tip: set GENIE_SPACE_ID={space_id} and WAREHOUSE_ID={wh.id} in the app's app.yaml (the app also "
      "auto-discovers both if left blank).")