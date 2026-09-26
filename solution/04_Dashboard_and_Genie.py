# Databricks notebook source
# MAGIC %md
# MAGIC # Solution · Dashboard and Genie
# MAGIC Creates a Genie space over the gold/silver tables and a starter AI/BI dashboard. Both are looked up by name
# MAGIC first, so re-running this notebook never creates duplicates.

# COMMAND ----------

# MAGIC %run ../00_Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json
w = WorkspaceClient()
whs = list(w.warehouses.list())
warehouse_id = (next((x.id for x in whs if "Serverless Starter" in (x.name or "")), None)
                or next(x.id for x in whs if x.enable_serverless_compute))
print("Warehouse:", warehouse_id)

GENIE_TITLE = "AI Summit BCN - LinkedIn"
DASH_NAME = "AI Summit BCN - LinkedIn Sentiment"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie space (via data-rooms — attaches tables directly)

# COMMAND ----------

existing = [s for s in w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
            if s.get("title") == GENIE_TITLE]
if existing:
    GENIE_SPACE_ID = existing[0]["space_id"]
    print("Genie space already exists:", GENIE_SPACE_ID)
else:
    space = w.api_client.do("POST", "/api/2.0/data-rooms", body={
        "display_name": GENIE_TITLE,
        "description": ("LinkedIn posts and comments on AI Summit Barcelona 2026: sentiment, topic, AI theme, "
                        "speaker/company/location, engagement and recommended actions."),
        "warehouse_id": warehouse_id,
        "run_as_type": "VIEWER",
        "table_identifiers": [f"{FQ}.gold_linkedin_metrics", f"{FQ}.silver_linkedin_enriched"],
    })
    GENIE_SPACE_ID = space.get("space_id") or space.get("id")
    print("Genie space id:", GENIE_SPACE_ID)
print("-> set this as the app's GENIE_SPACE_ID env var (or leave it blank: the app finds it by title).")
print("Paste the General Instructions from Module 04, Prompt 2 into the space's Instructions tab.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI/BI dashboard (datasets pre-aggregate; the medallion Gold stays per-item)

# COMMAND ----------

T = f"{FQ}.gold_linkedin_metrics"
TABLE_COLS = ["created_at", "item_type", "sentiment", "topic", "theme", "speaker", "company", "engagement",
              "recommended_action", "text_en", "original_text"]
serialized = {
  "datasets": [
    {"name": "by_sentiment", "displayName": "By sentiment",
     "queryLines": [f"SELECT sentiment, count(*) AS items FROM {T} GROUP BY sentiment"]},
    {"name": "by_theme", "displayName": "By AI theme",
     "queryLines": [f"SELECT theme, count(*) AS items FROM {T} WHERE theme IS NOT NULL GROUP BY theme ORDER BY items DESC"]},
    {"name": "by_action", "displayName": "By recommended action",
     "queryLines": [f"SELECT action_bucket, count(*) AS items FROM {T} WHERE action_bucket IS NOT NULL GROUP BY action_bucket ORDER BY items DESC"]},
    {"name": "recent", "displayName": "Recent LinkedIn posts",
     "queryLines": [f"SELECT {', '.join(TABLE_COLS)} FROM {T} ORDER BY created_at DESC LIMIT 100"]},
  ],
  "pages": [{"name": "main", "displayName": "AI Summit BCN · LinkedIn", "layout": [
    {"widget": {"name": "pie", "queries": [{"name": "q", "query": {"datasetName": "by_sentiment",
        "fields": [{"name": "sentiment", "expression": "`sentiment`"}, {"name": "items", "expression": "`items`"}], "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "pie", "encodings": {
            "angle": {"fieldName": "items", "scale": {"type": "quantitative"}, "displayName": "items"},
            "color": {"fieldName": "sentiment", "scale": {"type": "categorical"}, "displayName": "sentiment"}}}},
     "position": {"x": 0, "y": 0, "width": 2, "height": 6}},
    {"widget": {"name": "theme", "queries": [{"name": "q", "query": {"datasetName": "by_theme",
        "fields": [{"name": "theme", "expression": "`theme`"}, {"name": "items", "expression": "`items`"}], "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "bar", "encodings": {
            "x": {"fieldName": "items", "scale": {"type": "quantitative"}, "displayName": "items"},
            "y": {"fieldName": "theme", "scale": {"type": "categorical"}, "displayName": "AI theme"}}}},
     "position": {"x": 2, "y": 0, "width": 2, "height": 6}},
    {"widget": {"name": "action", "queries": [{"name": "q", "query": {"datasetName": "by_action",
        "fields": [{"name": "action_bucket", "expression": "`action_bucket`"}, {"name": "items", "expression": "`items`"}], "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "bar", "encodings": {
            "x": {"fieldName": "items", "scale": {"type": "quantitative"}, "displayName": "items"},
            "y": {"fieldName": "action_bucket", "scale": {"type": "categorical"}, "displayName": "recommended action"}}}},
     "position": {"x": 4, "y": 0, "width": 2, "height": 6}},
    {"widget": {"name": "tbl", "queries": [{"name": "q", "query": {"datasetName": "recent",
        "fields": [{"name": c, "expression": f"`{c}`"} for c in TABLE_COLS], "disaggregated": True}}],
        "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": [{"fieldName": c, "displayName": c} for c in TABLE_COLS]}}},
     "position": {"x": 0, "y": 6, "width": 6, "height": 8}},
  ]}],
}

body = {"display_name": DASH_NAME, "serialized_dashboard": json.dumps(serialized), "warehouse_id": warehouse_id}
dash = next((d for d in w.lakeview.list() if d.display_name == DASH_NAME and str(d.lifecycle_state) != "LifecycleState.TRASHED"), None)
if dash:
    did = dash.dashboard_id
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{did}", body=body)
    print("Dashboard updated:", did)
else:
    body["parent_path"] = f"/Users/{w.current_user.me().user_name}"
    did = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body=body)["dashboard_id"]
    print("Dashboard created:", did)
w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{did}/published",
                body={"warehouse_id": warehouse_id, "embed_credentials": True})
print("Dashboard published:", did)