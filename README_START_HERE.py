# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Start here — AI Summit BCN 2026 LinkedIn sentiment
# MAGIC
# MAGIC End-to-end social-sentiment demo on Databricks Free Edition using LinkedIn data.
# MAGIC
# MAGIC The AI Summit Barcelona team (22–23 September 2026, World Trade Center Barcelona) reads the LinkedIn
# MAGIC conversation about the summit to decide what to amplify, what to fix, and who to follow up with.
# MAGIC
# MAGIC > **Disclaimer:** The views, ideas, and content in this demo are my own and are based solely on publicly
# MAGIC > available information about Databricks. They do not represent Databricks, LinkedIn, the AI Summit
# MAGIC > Barcelona organisers or any employer, and are shared as-is for educational and demonstration purposes
# MAGIC > only. The bundled sample dataset is fictional.
# MAGIC
# MAGIC ## Modules (prompt-driven)
# MAGIC | Module | What it does |
# MAGIC |---|---|
# MAGIC | 00_Setup | Unity Catalog (schema + volume), config, medallion overview, shared LinkedIn helpers |
# MAGIC | 01_LinkedIn_Token | Store the LinkedIn token (or seed sample data), live 10-item pull, test the AI functions on real posts |
# MAGIC | 02_Pipeline | Medallion notebook: Bronze/Silver/Gold code + a prompt to schedule it as a 15-min job |
# MAGIC | 03_Preview_Tables | SELECT * on Bronze, Silver, Gold |
# MAGIC | 04_Dashboard_and_Genie | Prompt: generate the AI/BI dashboard + Genie space |
# MAGIC | 05_App | Prompt: deploy the single-page app (auto-refresh 15s) + a fallback permissions cell |
# MAGIC
# MAGIC ## solution/ folder
# MAGIC The same modules with **actual code** — a working reference you can run directly:
# MAGIC `solution/00_Setup`, `01_LinkedIn_Token`, `03_Preview_Tables`, `04_Dashboard_and_Genie`, and `05_app/`
# MAGIC (the Streamlit app). The Bronze/Silver/Gold code lives in `02_Pipeline` itself.
# MAGIC
# MAGIC ## Getting LinkedIn data — read this before the workshop
# MAGIC LinkedIn does **not** offer a public keyword or hashtag search API like X does, and scraping LinkedIn is
# MAGIC against its User Agreement. The pipeline supports the legitimate routes:
# MAGIC 1. **API** — posts from a LinkedIn Page you administer (e.g. the summit's Page) plus all comments on them,
# MAGIC    via the **Community Management API**. LinkedIn reviews access requests, so apply well before the event.
# MAGIC 2. **Volume** — drop JSON/CSV files into `/Volumes/workspace/ai_summit/linkedin_drop` (export from a
# MAGIC    licensed social-listening tool, or posts collected by hand). Columns: `item_id, item_type, parent_id,
# MAGIC    author, author_headline, text, lang, created_at, reactions, comments, reposts, url, source`
# MAGIC    (only `text` is required).
# MAGIC 3. **Sample** — Module 01 seeds 36 fictional posts/comments (EN/ES/CA) so every module works end to end.
# MAGIC
# MAGIC ## What changed vs the X/Twitter (Bruma · La Mercè) version
# MAGIC | Area | X version | LinkedIn version |
# MAGIC |---|---|---|
# MAGIC | Ingestion | `tweepy` `search_recent_tweets` + `since_id` | LinkedIn REST (`/rest/posts`, `/rest/socialActions/.../comments`, `/rest/socialMetadata`) or Volume files; MERGE on `item_id` |
# MAGIC | Credentials | Shared bearer token pasted in notebooks | Page-admin OAuth token kept in a Databricks secret |
# MAGIC | Unit of analysis | Tweet | Post **and** comment (`item_type`, `parent_id`) |
# MAGIC | Engagement | likes + retweets + replies (frozen) | reactions + comments + reposts, refreshed each run |
# MAGIC | AI enrichment | sentiment, topic, product/brand/location, translate, action | sentiment, topic, **AI theme**, speaker/company/location, translate, action |
# MAGIC | Action buckets | restock, logistics, promo, quality, monitor | amplify, fix_ops, follow_up, content, monitor |
# MAGIC | Privacy | author_id stored | member authors pseudonymised (SHA-256) in Bronze |
# MAGIC | Schedule | every 1 min | every 15 min (LinkedIn rate limits) |
# MAGIC | Tables | `workspace.bruma.*_social_*` | `workspace.ai_summit.bronze_linkedin_feed`, `silver_linkedin_enriched`, `gold_linkedin_metrics` |
# MAGIC
# MAGIC ## Make it yours
# MAGIC In 00_Setup, change `EVENT_NAME`, the dates, `TRACK_KEYWORDS` and `LI_ORG_URN` to your own event and
# MAGIC LinkedIn Page. Everything downstream follows.
# MAGIC
# MAGIC Medallion reference: https://www.databricks.com/blog/what-is-medallion-architecture
