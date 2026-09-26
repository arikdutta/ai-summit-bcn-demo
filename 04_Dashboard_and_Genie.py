# Databricks notebook source
# MAGIC %md
# MAGIC # Module 04 — Dashboard and Genie (prompts)
# MAGIC Two **separate** prompts. Run **Prompt 1 fully first** (wait until the dashboard is published),
# MAGIC then run **Prompt 2**. Keeping them apart stops the Assistant from bouncing between the two
# MAGIC assets and creating duplicates. A ready-made API implementation is in the **`solution/`** folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 1 — Dashboard (run this alone, first) — copy everything below
# MAGIC ```
# MAGIC Single, self-contained task: build ONE AI/BI (Lakeview) dashboard. Do NOT create a Genie
# MAGIC space, a job, or anything else. If a dashboard named "AI Summit BCN - LinkedIn Sentiment" already
# MAGIC exists, OPEN AND EDIT THAT ONE — never create a second dashboard. Stay on this one dashboard
# MAGIC canvas the whole time; do not navigate away until it is published.
# MAGIC
# MAGIC Data source: workspace.ai_summit.gold_linkedin_metrics on the Serverless Starter Warehouse.
# MAGIC
# MAGIC Add these 6 widgets on ONE page, each from its own SQL dataset:
# MAGIC
# MAGIC 1. Counter "LinkedIn posts & comments":
# MAGIC    SELECT COUNT(*) AS total_items FROM workspace.ai_summit.gold_linkedin_metrics
# MAGIC
# MAGIC 2. Pie chart "Sentiment mix" (count by sentiment). Colors: positive=#16a34a, negative=#dc2626,
# MAGIC    neutral=#64748b, mixed=#d97706:
# MAGIC    SELECT sentiment, COUNT(*) AS items FROM workspace.ai_summit.gold_linkedin_metrics
# MAGIC    GROUP BY sentiment
# MAGIC
# MAGIC 3. Line chart "Sentiment over time" (cumulative running total per hour, one line per sentiment):
# MAGIC    SELECT hour, sentiment,
# MAGIC           SUM(c) OVER (PARTITION BY sentiment ORDER BY hour) AS cumulative_items
# MAGIC    FROM (SELECT date_trunc('hour', created_at) AS hour, sentiment, COUNT(*) AS c
# MAGIC          FROM workspace.ai_summit.gold_linkedin_metrics GROUP BY 1, sentiment)
# MAGIC    ORDER BY hour
# MAGIC
# MAGIC 4. Bar chart "AI themes" (count by theme, colored by sentiment, sorted desc):
# MAGIC    SELECT theme, sentiment, COUNT(*) AS items FROM workspace.ai_summit.gold_linkedin_metrics
# MAGIC    GROUP BY theme, sentiment ORDER BY items DESC
# MAGIC
# MAGIC 5. Bar chart "Recommended actions" (count by action_bucket, sorted desc):
# MAGIC    SELECT action_bucket, COUNT(*) AS items FROM workspace.ai_summit.gold_linkedin_metrics
# MAGIC    WHERE action_bucket IS NOT NULL GROUP BY action_bucket ORDER BY items DESC
# MAGIC
# MAGIC 6. Table "Recent LinkedIn posts":
# MAGIC    SELECT created_at, item_type, sentiment, topic, theme, speaker, company, engagement,
# MAGIC           action_bucket, recommended_action, text_en, original_text, url
# MAGIC    FROM workspace.ai_summit.gold_linkedin_metrics ORDER BY created_at DESC LIMIT 50
# MAGIC
# MAGIC Then PUBLISH the dashboard, print its URL, and STOP. Do not create anything else.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt 2 — Genie space (run this alone, second) — copy everything below
# MAGIC The instructions below are deliberately precise (exact column values + a few trusted example
# MAGIC queries). This is what stops Genie from hallucinating columns/values and gives real answers.
# MAGIC ```
# MAGIC Single, self-contained task: create ONE Genie space. Do NOT create or edit any dashboard.
# MAGIC If a Genie space named "AI Summit BCN - LinkedIn" already exists, OPEN AND EDIT THAT ONE — never
# MAGIC create a second space. Do NOT add sample/suggested question CHIPS (they slow creation) —
# MAGIC but DO add the trusted example SQL queries listed at the end (those improve accuracy).
# MAGIC
# MAGIC Attach ONE primary table: workspace.ai_summit.gold_linkedin_metrics (one row per LinkedIn post or
# MAGIC comment — always answer from it). Also attach workspace.ai_summit.silver_linkedin_enriched for raw detail only.
# MAGIC
# MAGIC Set the General Instructions to EXACTLY this:
# MAGIC "You answer questions about LinkedIn posts and comments on AI Summit Barcelona 2026 (22-23 September 2026,
# MAGIC World Trade Center Barcelona) for the summit's community and operations team. Answer ONLY from the
# MAGIC attached tables. Never invent columns or values; if a question needs data that is not present, say so
# MAGIC plainly. Prefer workspace.ai_summit.gold_linkedin_metrics (one row per post or comment). Its columns:
# MAGIC - item_id (string); item_type (string): 'post' or 'comment'; parent_id (string, the post a comment belongs to).
# MAGIC - created_at (timestamp, UTC — use it for time filters like day 1 / day 2 / last hour).
# MAGIC - sentiment (string): EXACTLY one of 'positive','negative','neutral','mixed' (lowercase).
# MAGIC - topic (string): one of 'session praise','session complaint','logistics','networking & hiring',
# MAGIC   'sponsor & lead signal','general chatter'.
# MAGIC - theme (string): one of 'agents','genai & llms','data & infrastructure','ethics & regulation',
# MAGIC   'industry use cases','startups & funding','talent & careers','other'.
# MAGIC - speaker, company, location (string, often NULL — extracted entities; always exclude NULL when grouping).
# MAGIC - engagement (int): reactions + comments + reposts (refreshed every pipeline run).
# MAGIC - author_headline (string, may be NULL); text_en (string, English); original_text (string, original
# MAGIC   language: English, Spanish or Catalan); url (string); source (string: 'linkedin_api','file','sample').
# MAGIC - action_bucket (string): one of 'amplify','fix_ops','follow_up','content','monitor'.
# MAGIC - recommended_action (string): a one-line suggestion for the summit team.
# MAGIC Guidance: 'what resonated' = rank theme (or speaker) by count weighted by positive share and engagement;
# MAGIC 'problems' = sentiment='negative' OR topic IN ('session complaint','logistics'); 'leads' =
# MAGIC action_bucket='follow_up' OR topic='sponsor & lead signal'; 'what to do' = group by action_bucket;
# MAGIC 'mood/how are we doing' = share of each sentiment. Day 1 = 2026-09-22, day 2 = 2026-09-23 (UTC dates)."
# MAGIC
# MAGIC Then add these trusted example SQL queries (name -> SQL) to the space:
# MAGIC 1. Sentiment breakdown:
# MAGIC    SELECT sentiment, count(*) AS items FROM workspace.ai_summit.gold_linkedin_metrics
# MAGIC    GROUP BY sentiment ORDER BY items DESC
# MAGIC 2. Which themes resonated most:
# MAGIC    SELECT theme, count(*) AS mentions, sum(engagement) AS engagement,
# MAGIC           round(avg(CASE WHEN sentiment='positive' THEN 1.0 ELSE 0 END), 2) AS positive_share
# MAGIC    FROM workspace.ai_summit.gold_linkedin_metrics
# MAGIC    GROUP BY theme ORDER BY mentions * positive_share DESC
# MAGIC 3. Top attendee problems:
# MAGIC    SELECT topic, recommended_action, text_en, engagement FROM workspace.ai_summit.gold_linkedin_metrics
# MAGIC    WHERE sentiment='negative' OR topic IN ('session complaint','logistics')
# MAGIC    ORDER BY engagement DESC LIMIT 20
# MAGIC 4. Leads to follow up:
# MAGIC    SELECT created_at, company, recommended_action, text_en, url FROM workspace.ai_summit.gold_linkedin_metrics
# MAGIC    WHERE action_bucket='follow_up' OR topic='sponsor & lead signal' ORDER BY created_at DESC
# MAGIC 5. Recommended actions right now:
# MAGIC    SELECT action_bucket, count(*) AS items FROM workspace.ai_summit.gold_linkedin_metrics
# MAGIC    GROUP BY action_bucket ORDER BY items DESC
# MAGIC
# MAGIC Then save, print the Genie space id, and STOP. Do not create anything else.
# MAGIC ```
# MAGIC
# MAGIC Sample questions to try live (from the runbook doc — don't seed them as chips):
# MAGIC *What resonated most on LinkedIn? · What were the biggest attendee complaints? · Which sponsor or hiring
# MAGIC leads should we follow up? · How did sentiment change from day 1 to day 2?*