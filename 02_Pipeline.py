# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Module 02 — Medallion pipeline (Bronze -> Silver -> Gold)
# MAGIC A plain, fast medallion (no SDP). Reads LinkedIn via the API (token from the secret stored in **Module 01**)
# MAGIC or from the Volume drop folder, and builds all three layers in one run. Re-run to ingest newer posts and
# MAGIC comments; the last cell shows how to schedule it.
# MAGIC
# MAGIC - Ask Genie Code to deploy as per prompt in the last cell before giving Walkthrough

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

MODE = resolve_source_mode()
if MODE == "api" and not (get_linkedin_token() and LI_ORG_URN):
    raise ValueError("API mode needs the token (Module 01, Option A) and LI_ORG_URN in 00_Setup.")
print("Source mode:", MODE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — raw LinkedIn posts and comments
# MAGIC One row per post or comment in `bronze_linkedin_feed`. We **MERGE** on `item_id`: new items are inserted,
# MAGIC existing ones only get their engagement counts refreshed (LinkedIn reactions keep growing for days, and
# MAGIC there is no `since_id` like on X). Member authors are pseudonymised before they land here.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.bronze_linkedin_feed (
  item_id STRING, item_type STRING, parent_id STRING, author STRING, author_headline STRING,
  text STRING, lang STRING, created_at TIMESTAMP, reactions INT, comments INT, reposts INT,
  url STRING, source STRING, ingestion_time TIMESTAMP)
COMMENT 'Raw LinkedIn posts and comments about {EVENT_NAME} (bronze)'""")


def read_volume():
    """All JSON / CSV files in the drop folder, normalised to the Bronze schema."""
    raw = ("item_id STRING, item_type STRING, parent_id STRING, author STRING, author_headline STRING, "
           "text STRING, lang STRING, created_at STRING, reactions INT, comments INT, reposts INT, "
           "url STRING, source STRING")
    files = [f.path for f in dbutils.fs.ls(VOLUME_PATH)]
    parts = []
    js = [p for p in files if p.endswith((".json", ".jsonl"))]
    cs = [p for p in files if p.endswith(".csv")]
    if js:
        parts.append(spark.read.schema(raw).json(js))
    if cs:
        parts.append(spark.read.schema(raw).option("header", True).option("multiLine", True)
                     .option("escape", '"').csv(cs))
    if not parts:
        return None
    df = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    kw = "(?i)(" + "|".join(re.escape(k) for k in TRACK_KEYWORDS) + ")"
    df = (df.filter(F.col("text").isNotNull() & (F.trim("text") != ""))
            .withColumn("source", F.coalesce("source", F.lit("file")))
            .withColumn("item_type", F.coalesce("item_type", F.lit("post")))
            .withColumn("item_id", F.coalesce("item_id", F.sha2(F.concat_ws("|", "author", "text"), 256)))
            .withColumn("created_at", F.coalesce(F.expr("try_to_timestamp(created_at)"), F.current_timestamp()))
            .withColumn("author", F.when(F.col("author").startswith("urn:li:organization"), F.col("author"))
                                   .otherwise(F.substring(F.sha2(F.coalesce("author", F.lit("")), 256), 1, 16)))
            # keyword filter only for posts from real exports; comments sit on event posts already
            .filter((F.col("source") != "file") | (F.col("item_type") == "comment") | F.col("text").rlike(kw)))
    for c in ("reactions", "comments", "reposts"):
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    return df.select(*BRONZE_COLS)


if MODE == "api":
    _rows = li_fetch(get_linkedin_token(), LI_ORG_URN)
    incoming = spark.createDataFrame(_rows, BRONZE_SCHEMA) if _rows else None
else:
    incoming = read_volume()

if incoming is not None:
    incoming = incoming.dropDuplicates(["item_id"])
    incoming.createOrReplaceTempView("_incoming")
    _new = spark.sql(f"""SELECT count(*) n FROM _incoming i
                         LEFT ANTI JOIN {FQ}.bronze_linkedin_feed b ON i.item_id = b.item_id""").collect()[0]["n"]
    spark.sql(f"""
    MERGE INTO {FQ}.bronze_linkedin_feed t
    USING (SELECT *, current_timestamp() AS ingestion_time FROM _incoming) s
    ON t.item_id = s.item_id
    WHEN MATCHED THEN UPDATE SET t.reactions = s.reactions, t.comments = s.comments, t.reposts = s.reposts
    WHEN NOT MATCHED THEN INSERT *""")
    print(f"Bronze: +{_new} new items (engagement refreshed on the rest)")
else:
    print("Bronze: nothing to ingest (empty API response or empty Volume — seed sample data in Module 01).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — AI-enriched
# MAGIC Enrich only the new items (LEFT ANTI JOIN, so each post/comment is scored once). Managed AI functions add
# MAGIC sentiment / topic / AI theme / entities / English, and one custom `ai_query` returns JSON
# MAGIC `{bucket, action}` that we parse into `action_bucket` and `recommended_action`.

# COMMAND ----------

TOPICS = ['session praise', 'session complaint', 'logistics', 'networking & hiring',
          'sponsor & lead signal', 'general chatter']
THEMES = ['agents', 'genai & llms', 'data & infrastructure', 'ethics & regulation', 'industry use cases',
          'startups & funding', 'talent & careers', 'other']
_arr = lambda xs: ", ".join(f"'{x}'" for x in xs)

ACTION_PROMPT = ('You are the community and operations lead for AI Summit Barcelona 2026 (22-23 September, World '
                 'Trade Center Barcelona). Reply with ONLY compact JSON with two keys: "bucket" (one of: amplify, '
                 'fix_ops, follow_up, content, monitor) and "action" (one short imperative action, max 12 words). '
                 'amplify = reshare or thank a happy attendee or speaker; fix_ops = a venue or logistics problem; '
                 'follow_up = a sponsor, hiring, partnership or sales lead; content = a request for slides, '
                 'recordings or recaps; monitor = nothing to do. '
                 'Example. Post: "Registration queue at the WTC took 45 minutes and the main auditorium closed its '
                 'doors before the keynote." Response: {"bucket": "fix_ops", "action": "Add badge desks and an '
                 'overflow room with live stream"}. Now classify this LinkedIn post: ')
assert "'" not in ACTION_PROMPT, "No single quotes in the prompt (it is embedded in a SQL string literal)."


def _enrich(src):
    return f"""
    SELECT item_id, item_type, parent_id, created_at, author_headline, lang, source, url, original_text,
      sentiment, topic, theme, entities, text_en,
      get_json_object(_action_json, '$.bucket') AS action_bucket,
      get_json_object(_action_json, '$.action') AS recommended_action
    FROM (
      SELECT *, regexp_extract(_action, '(?s)[{{].*[}}]', 0) AS _action_json
      FROM (
        SELECT item_id, item_type, parent_id, created_at, author_headline, lang, source, url,
          text AS original_text,
          ai_analyze_sentiment(text) AS sentiment,
          ai_classify(text, ARRAY({_arr(TOPICS)})) AS topic,
          ai_classify(text, ARRAY({_arr(THEMES)})) AS theme,
          ai_extract(text, ARRAY('speaker','company','location')) AS entities,
          CASE WHEN lang = 'en' THEN text ELSE ai_translate(text, 'en') END AS text_en,
          ai_query('databricks-meta-llama-3-3-70b-instruct', '{ACTION_PROMPT}' || text) AS _action
        FROM {src}
      )
    )"""


spark.sql(f"CREATE TABLE IF NOT EXISTS {FQ}.silver_linkedin_enriched AS "
          f"{_enrich(FQ + '.bronze_linkedin_feed')} WHERE 1=0")
_new = (f"(SELECT b.* FROM {FQ}.bronze_linkedin_feed b "
        f"LEFT ANTI JOIN {FQ}.silver_linkedin_enriched s ON b.item_id = s.item_id)")
spark.sql(f"INSERT INTO {FQ}.silver_linkedin_enriched {_enrich(_new)}")
print("Silver: enriched new posts and comments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — presentation-ready
# MAGIC Flatten the entity struct and expose clean, per-item columns for BI. Engagement comes from Bronze, so it
# MAGIC stays current as reactions keep arriving. No aggregation here — the dashboard, Genie and app aggregate on
# MAGIC top of `gold_linkedin_metrics`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.gold_linkedin_metrics
COMMENT 'One row per LinkedIn post or comment about {EVENT_NAME}, AI-enriched (gold)' AS
SELECT s.item_id, s.item_type, s.parent_id, s.created_at, s.sentiment, s.topic, s.theme,
       s.entities.speaker AS speaker, s.entities.company AS company, s.entities.location AS location,
       coalesce(b.reactions, 0) + coalesce(b.comments, 0) + coalesce(b.reposts, 0) AS engagement,
       s.author_headline, s.text_en, s.original_text, s.action_bucket, s.recommended_action, s.url, s.source
FROM {FQ}.silver_linkedin_enriched s
LEFT JOIN {FQ}.bronze_linkedin_feed b ON s.item_id = b.item_id
""")
display(spark.sql(f"SELECT created_at, item_type, sentiment, topic, theme, action_bucket, recommended_action, "
                  f"text_en FROM {FQ}.gold_linkedin_metrics ORDER BY created_at DESC LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make it live — schedule this notebook every 15 minutes (prompt)
# MAGIC LinkedIn moves slower than X and its API is rate-limited per app and per day (one run = 1 posts call +
# MAGIC 2 calls per post), so every 15 minutes is plenty — the app still auto-refreshes every 15s from Gold.
# MAGIC Paste into the Genie Code:
# MAGIC ```
# MAGIC Create a Databricks Job named "ai_summit_linkedin_pipeline" that runs THIS notebook (02_Pipeline) on serverless.
# MAGIC 1. One task, notebook = this 02_Pipeline notebook.
# MAGIC 2. Schedule: every 15 minutes (cron "0 0/15 * * * ?"), timezone Europe/Madrid; max_concurrent_runs = 1.
# MAGIC 3. The LinkedIn token is read from the Databricks secret ai_summit/linkedin_token (stored in Module 01),
# MAGIC    so nothing needs to be pasted. Do NOT put the token in the job definition.
# MAGIC 4. Start it, confirm the first run succeeds, and print the job URL. Pause the job after the demo.
# MAGIC ```