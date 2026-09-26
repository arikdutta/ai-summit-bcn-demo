# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Social Sentiment on Databricks — Setup
# MAGIC ### AI Summit Barcelona 2026 · LinkedIn Pulse
# MAGIC
# MAGIC **Business context (AI Summit BCN):**
# MAGIC - AI Summit Barcelona 2026 runs **22–23 September 2026** at the **World Trade Center Barcelona** (Port of
# MAGIC   Barcelona), the centrepiece of Barcelona AI Week: 10,000+ attendees, ~200 speakers, a 500-builder
# MAGIC   hackathon and ~50 side events across the city. Motto: *Beyond Theory. AI in Action.*
# MAGIC - The professional conversation about the summit lives on **LinkedIn**, not X: speakers post their decks,
# MAGIC   attendees post recaps, sponsors post booth photos, and the recap wave keeps going for days afterwards.
# MAGIC - Goal: read that LinkedIn conversation to decide, in near real time:
# MAGIC   - which sessions, speakers and AI themes resonated (to **amplify** and clip for content),
# MAGIC   - where the attendee pain points are (registration queues, full rooms, wifi, hackathon logistics),
# MAGIC   - which comments are sponsor / hiring / partnership signals worth a **follow-up**.
# MAGIC - How: a Medallion pipeline (Bronze -> Silver -> Gold) enriching each LinkedIn post and comment with
# MAGIC   Databricks AI functions (sentiment, topic, theme, entities, translation), served through an AI/BI
# MAGIC   dashboard, Genie, and an app.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event
# MAGIC <img src="img/summit_logo.png" width="360"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Create a Databricks Free Edition workspace
# MAGIC 1. Go to **https://www.databricks.com/learn/free-edition** and sign up (or sign in).
# MAGIC 2. Open the workspace. Everything in this demo runs on serverless compute included with Free Edition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Unity Catalog: catalog, schema and volume
# MAGIC Unity Catalog is the single governance layer for all data and AI models in this demo.
# MAGIC Reference: **https://docs.databricks.com/data-governance/unity-catalog/index.html**
# MAGIC
# MAGIC **Why Unity Catalog:**
# MAGIC - One governance layer across workspaces: tables, files, ML models, dashboards and secrets in a single
# MAGIC   three-level namespace (`catalog.schema.object`).
# MAGIC - Fine-grained access control (GRANT/REVOKE), plus built-in lineage, audit, search and tagging.
# MAGIC - Governs AI too: the foundation models and serving endpoints we call are secured by Unity Catalog.
# MAGIC - A **Volume** (`linkedin_drop`) holds LinkedIn exports / sample files that the pipeline ingests.
# MAGIC
# MAGIC **Why ABAC (attribute-based access control):**
# MAGIC - Define access by tags/attributes once (e.g., "PII", "region=EU") and it applies everywhere, instead
# MAGIC   of managing grants table-by-table.
# MAGIC - Enables row filters and column masking driven by attributes — important here: LinkedIn content is
# MAGIC   tied to real, identifiable professionals (EU / GDPR). The pipeline already pseudonymises member
# MAGIC   authors in Bronze; ABAC lets you mask the free text too.
# MAGIC - Scales governance as the number of tables and users grows.

# COMMAND ----------

# Configuration: catalog, schema, volume
CATALOG = "workspace"        # default catalog
SCHEMA  = "ai_summit"        # demo schema
VOLUME  = "linkedin_drop"    # drop folder for LinkedIn exports / sample data
FQ      = f"{CATALOG}.{SCHEMA}"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print("Target catalog.schema:", FQ, "| volume:", VOLUME_PATH)

# COMMAND ----------

# What we track. Change these to run the demo on your own event.
EVENT_NAME   = "AI Summit Barcelona 2026"
COMPANY      = "AI Summit BCN"            # whose ops/community team we play in the demo
EVENT_START  = "2026-09-22"
EVENT_END    = "2026-09-23"
LOOKBACK_FROM = "2026-09-01"              # ignore LinkedIn posts older than this

# LinkedIn has NO public keyword/hashtag search API (unlike X). Two supported sources:
#   "api"    -> posts published by a LinkedIn Page you administer + the comments on them
#               (Community Management API; needs an approved app and a Page-admin access token)
#   "volume" -> JSON/CSV files dropped into the Volume above (licensed social-listening export,
#               hand-collected posts, or the sample data seeded in Module 01)
#   "auto"   -> "api" when a token and LI_ORG_URN are set, otherwise "volume"
SOURCE_MODE = "auto"
LI_ORG_URN  = ""   # e.g. "urn:li:organization:12345678" — the summit (or sponsor) LinkedIn Page you administer

# Keyword filter applied to *posts* coming from files (comments on event posts are always kept).
# Check the organisers' official hashtag and add it here.
TRACK_KEYWORDS = ["AI Summit", "AISummit", "AISummitBCN", "AISummitBarcelona", "AI Week",
                  "AIWeek", "Beyond Theory", "World Trade Center Barcelona", "WTC Barcelona"]

# Model for the custom ai_query task (Databricks-hosted foundation model).
AI_MODEL = "system.ai.claude-opus-4-7"
print("Event:", EVENT_NAME, "| source mode:", SOURCE_MODE, "| keywords:", TRACK_KEYWORDS)

# COMMAND ----------

# Create the catalog context, schema and volume
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT 'AI Summit BCN 2026 LinkedIn sentiment demo'")
spark.sql(f"USE SCHEMA {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.{VOLUME} COMMENT 'LinkedIn exports and sample files for the pipeline'")
spark.conf.set("spark.sql.session.timeZone", "UTC")   # all timestamps stored in UTC; the app shows Barcelona time
print("Schema ready:", FQ)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Medallion architecture
# MAGIC Reference: https://www.databricks.com/blog/what-is-medallion-architecture
# MAGIC
# MAGIC <img src="img/medallion_architecture.png" width="1000"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Shared LinkedIn helpers (used by Modules 01 and 02)
# MAGIC - Token is read from a **Databricks secret** (`ai_summit / linkedin_token`) so the scheduled job works
# MAGIC   headlessly; Module 01 stores it for you. LinkedIn tokens are Page-admin OAuth tokens (valid ~60 days),
# MAGIC   so unlike the shared X bearer token they should never be pasted into a notebook that gets shared.
# MAGIC - Every REST call sends the required `LinkedIn-Version` (YYYYMM) and `X-Restli-Protocol-Version` headers.
# MAGIC - Member authors are pseudonymised (SHA-256) before they land in Bronze.

# COMMAND ----------

import re, time, hashlib, urllib.parse
from datetime import datetime, timezone
import requests
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

SECRET_SCOPE, SECRET_KEY = "ai_summit", "linkedin_token"
LI_API = "https://api.linkedin.com/rest"
LI_API_VERSION = "202608"   # YYYYMM; LinkedIn supports each version for ~1 year — bump when it sunsets

BRONZE_SCHEMA = StructType([
    StructField("item_id", StringType()), StructField("item_type", StringType()),
    StructField("parent_id", StringType()), StructField("author", StringType()),
    StructField("author_headline", StringType()), StructField("text", StringType()),
    StructField("lang", StringType()), StructField("created_at", TimestampType()),
    StructField("reactions", IntegerType()), StructField("comments", IntegerType()),
    StructField("reposts", IntegerType()), StructField("url", StringType()),
    StructField("source", StringType()),
])
BRONZE_COLS = [f.name for f in BRONZE_SCHEMA.fields]


def get_linkedin_token():
    """Secret scope first (works in jobs), then the driver stash written by Module 01."""
    try:
        return dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
    except Exception:
        pass
    try:
        return open("/tmp/ai_summit_li_token").read().strip() or None
    except Exception:
        return None


def resolve_source_mode():
    if SOURCE_MODE in ("api", "volume"):
        return SOURCE_MODE
    return "api" if (get_linkedin_token() and LI_ORG_URN) else "volume"


def pseudonymise(author):
    """Keep organisation URNs readable; hash member identifiers (GDPR-friendly)."""
    if not author or str(author).startswith("urn:li:organization"):
        return author
    return hashlib.sha256(str(author).encode()).hexdigest()[:16]


def li_clean(text):
    """LinkedIn 'little text' -> plain text: {hashtag|\\#|AI} -> #AI, @[Name](urn:li:...) -> @Name, unescape."""
    if not text:
        return text
    text = re.sub(r"\{hashtag\|\\?#\|([^}]*)\}", r"#\1", text)
    text = re.sub(r"@\[([^\]]+)\]\(urn:li:[^)]+\)", r"@\1", text)
    text = re.sub(r"\\([\\()\[\]{}@#*_~<>|])", r"\1", text)
    return text.strip()


def _enc(urn):
    return urllib.parse.quote(urn, safe="")


def _ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None) if ms else None


def li_get(path, token, query=""):
    url = f"{LI_API}/{path}" + (f"?{query}" if query else "")
    headers = {"Authorization": f"Bearer {token}", "LinkedIn-Version": LI_API_VERSION,
               "X-Restli-Protocol-Version": "2.0.0"}
    for attempt in range(4):
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 429:            # throttled: back off and retry
            time.sleep(5 * 2 ** attempt)
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"LinkedIn API {r.status_code} on {path}: {r.text[:300]}")
        return r.json()
    raise RuntimeError(f"LinkedIn API still throttling on {path} after retries")


def li_social_metadata(token, urn):
    try:
        m = li_get(f"socialMetadata/{_enc(urn)}", token)
        reactions = sum(int((v or {}).get("count", 0)) for v in (m.get("reactionSummaries") or {}).values())
        comments = int((m.get("commentSummary") or {}).get("count", 0))
        return reactions, comments
    except Exception:
        return 0, 0


def li_fetch(token, org_urn, since=None, max_posts=50, max_comments=100, with_comments=True):
    """Posts authored by the Page + top-level comments on them. Returns rows in BRONZE_COLS order."""
    since = since or datetime.fromisoformat(LOOKBACK_FROM)
    rows = []
    data = li_get("posts", token, f"author={_enc(org_urn)}&q=author&count={max_posts}&sortBy=LAST_MODIFIED")
    for p in data.get("elements", []):
        pid = p.get("id")
        created = _ms(p.get("publishedAt") or p.get("createdAt"))
        text = li_clean(p.get("commentary") or "")
        if not pid or (created and created < since):
            continue
        if text:
            reactions, n_comments = li_social_metadata(token, pid)
            rows.append((pid, "post", None, pseudonymise(p.get("author")), None, text, None, created,
                         reactions, n_comments, 0, f"https://www.linkedin.com/feed/update/{pid}/", "linkedin_api"))
        if not with_comments:
            continue
        try:
            cs = li_get(f"socialActions/{_enc(pid)}/comments", token, f"count={max_comments}")
        except Exception as e:
            print(f"  comments skipped for {pid}: {e}")
            continue
        for c in cs.get("elements", []):
            ctext = li_clean((c.get("message") or {}).get("text") or "")
            if not ctext:
                continue
            cid = c.get("commentUrn") or f"{pid}#comment:{c.get('id')}"
            likes = int(((c.get("likesSummary") or {}).get("totalLikes")) or 0)
            replies = int(((c.get("commentsSummary") or {}).get("aggregatedTotalComments")) or 0)
            rows.append((cid, "comment", pid, pseudonymise(c.get("actor")), None, ctext, None,
                         _ms((c.get("created") or {}).get("time")), likes, replies, 0,
                         f"https://www.linkedin.com/feed/update/{pid}/", "linkedin_api"))
    return rows


print("LinkedIn helpers ready. Resolved source mode:", resolve_source_mode())

# COMMAND ----------

# MAGIC %md
# MAGIC Setup complete. Next: **01_LinkedIn_Token** to connect LinkedIn (or seed sample data) and verify the AI
# MAGIC functions on real posts, then **02** for the pipeline.
