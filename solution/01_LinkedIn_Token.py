# Databricks notebook source
# MAGIC %md
# MAGIC # Module 01 — LinkedIn token, live connection, and AI test
# MAGIC
# MAGIC Unlike X, LinkedIn has **no public search API** for hashtags or keywords, and scraping LinkedIn breaks
# MAGIC its User Agreement. So this demo reads LinkedIn in the two ways LinkedIn supports:
# MAGIC
# MAGIC | Option | Source | What you need |
# MAGIC |---|---|---|
# MAGIC | **A — API** | Posts published by the summit's LinkedIn Page + every comment on them | An app with the **Community Management API** product approved, and an access token from a Page admin with the `r_organization_social` read scope |
# MAGIC | **B — Volume** | JSON/CSV files dropped into `/Volumes/workspace/ai_summit/linkedin_drop` | An export from a licensed social-listening tool, or posts your team collected by hand |
# MAGIC | **C — Sample** | Seeds a fictional sample file into the Volume (Option B path) | Nothing — use it to rehearse or when API approval is still pending |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option A — Store the LinkedIn access token (once)
# MAGIC Get it from **linkedin.com/developers** -> your app -> **Auth** -> OAuth 2.0 tools (sign in as a Page admin,
# MAGIC tick the organisation read scopes). Paste it below and run the cell: it is saved to the Databricks secret
# MAGIC `ai_summit / linkedin_token` (so the scheduled pipeline in Module 02 can read it headlessly) and then you
# MAGIC can **clear the cell**. Also set `LI_ORG_URN` in 00_Setup (the Page's `urn:li:organization:<id>`; the id is
# MAGIC in the Page admin URL `linkedin.com/company/<id>/admin`).

# COMMAND ----------

LI_ACCESS_TOKEN = "PASTE_YOUR_TOKEN_HERE"   # clear this line again after running the cell

if LI_ACCESS_TOKEN and not LI_ACCESS_TOKEN.startswith("PASTE_"):
    from databricks.sdk import WorkspaceClient
    _w = WorkspaceClient()
    if SECRET_SCOPE not in [s.name for s in _w.secrets.list_scopes()]:
        _w.secrets.create_scope(scope=SECRET_SCOPE)
    _w.secrets.put_secret(scope=SECRET_SCOPE, key=SECRET_KEY, string_value=LI_ACCESS_TOKEN)
    open("/tmp/ai_summit_li_token", "w").write(LI_ACCESS_TOKEN)   # driver stash for this session
    print(f"Token stored in secret {SECRET_SCOPE}/{SECRET_KEY}. Now clear LI_ACCESS_TOKEN above.")
else:
    print("No token pasted — fine if a token is already stored, or if you use Option B/C.")
print("Token available:", bool(get_linkedin_token()), "| LI_ORG_URN:", LI_ORG_URN or "(not set)",
      "| mode:", resolve_source_mode())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option C — Seed the sample dataset (rehearsal / no API access yet)
# MAGIC Writes ~36 **fictional** LinkedIn-style posts and comments (English, Spanish, Catalan; authors are
# MAGIC placeholders, companies are invented) into the Volume, tagged `source = 'sample'` so the dashboard and app
# MAGIC can flag them. Skip this cell when you run on real data — or delete the file from the Volume afterwards.

# COMMAND ----------

SEED_SAMPLE = True   # set False once real data flows

_S = [  # (type, parent, headline, lang, created_at UTC, reactions, comments, reposts, text)
 ("post", None, "Founder, health-AI startup", "en", "2026-09-21T16:05:00Z", 212, 18, 6,
  "Landing in Barcelona for #AISummitBCN tomorrow. Two days at the World Trade Center, 10k people who actually ship AI. Who else is going? Let's meet at the hackathon area."),
 ("post", None, "Head of Data, retail group", "en", "2026-09-22T07:40:00Z", 96, 11, 2,
  "Registration at the WTC Barcelona took almost 45 minutes this morning. Great energy inside, but the badge pickup needs more desks. #AISummit"),
 ("comment", "sample:002", "Event producer", "en", "2026-09-22T07:58:00Z", 14, 0, 0,
  "Same here, the queue went all the way along the Port Vell promenade. Missed the first ten minutes of the opening keynote."),
 ("post", None, "ML Engineer", "es", "2026-09-22T09:15:00Z", 340, 27, 12,
  "La keynote de apertura sobre agentes en producción ha sido lo mejor que he visto este año. Nada de humo: arquitecturas reales, métricas reales. #AISummitBarcelona"),
 ("post", None, "CTO, logistics scale-up", "en", "2026-09-22T09:50:00Z", 128, 9, 3,
  "The main auditorium was full 15 minutes before the agents keynote and they closed the doors. Please add an overflow room with a live stream next year."),
 ("comment", "sample:005", "Product Manager", "en", "2026-09-22T10:02:00Z", 22, 1, 0,
  "+1, a lot of us ended up watching through the glass. Content was excellent though."),
 ("post", None, "Investigadora en IA", "ca", "2026-09-22T10:30:00Z", 187, 14, 5,
  "Quin orgull veure Barcelona com a hub europeu d'IA. El panell sobre IA en salut al World Trade Center ha estat brillant, molt aplicat i gens teòric. #AIWeek"),
 ("post", None, "Partner, early-stage VC", "en", "2026-09-22T11:20:00Z", 265, 31, 9,
  "Three things I heard at AI Summit Barcelona: inference cost is the new cloud bill, evals beat vibes, and small domain models are winning in regulated industries. Bullish on the Barcelona ecosystem."),
 ("comment", "sample:008", "Data Scientist", "en", "2026-09-22T11:45:00Z", 17, 0, 0,
  "The evals point is so true. Would love the slides from that session."),
 ("post", None, "Head of Talent", "en", "2026-09-22T12:10:00Z", 74, 22, 4,
  "We are hiring ML and platform engineers in Barcelona. Come find the Datavora team at booth B12 at the AI Summit, or DM me."),
 ("comment", "sample:010", "Senior ML Engineer", "en", "2026-09-22T12:35:00Z", 5, 1, 0,
  "Just stopped by B12, great chat. Sending my CV this afternoon."),
 ("post", None, "Desarrollador backend", "es", "2026-09-22T13:05:00Z", 41, 12, 1,
  "El wifi del recinto se ha caído durante casi una hora justo en los workshops prácticos. Imposible seguir la demo en vivo. #AISummitBCN"),
 ("comment", "sample:012", "Asistente", "es", "2026-09-22T13:20:00Z", 9, 0, 0,
  "Igual en la sala 3. Al final compartimos datos del móvil entre varios."),
 ("post", None, "Hackathon mentor", "en", "2026-09-22T15:00:00Z", 402, 38, 21,
  "500 builders in one room for the AI Summit hackathon. The level is insane: teams shipping agentic workflows for port logistics and healthcare triage in under 24 hours."),
 ("comment", "sample:014", "Student, UPC", "en", "2026-09-22T15:25:00Z", 11, 0, 0,
  "First hackathon ever and the mentors have been amazing. Coffee ran out at 3pm though."),
 ("post", None, "Marketing Director", "en", "2026-09-22T17:30:00Z", 88, 7, 2,
  "Food lines at lunch were long and prices steep for what it was. Content 9/10, catering 5/10. #AISummit"),
 ("post", None, "Responsable de innovación, sector público", "es", "2026-09-22T18:10:00Z", 156, 16, 6,
  "Muy interesante el debate sobre regulación y la AI Act. Por fin una conversación práctica sobre cómo cumplir sin frenar la innovación."),
 ("post", None, "AI consultant", "en", "2026-09-22T20:45:00Z", 133, 10, 3,
  "Great side event at a rooftop in Poblenou for AI Week. Barcelona does networking better than any other tech city in Europe."),
 ("post", None, "CEO, fintech", "en", "2026-09-23T08:20:00Z", 71, 5, 1,
  "Day 2 of AI Summit Barcelona. Registration took two minutes today, well done to the team for fixing it overnight."),
 ("post", None, "Enginyer de dades", "ca", "2026-09-23T09:40:00Z", 64, 6, 1,
  "La xerrada sobre infraestructura de dades per a LLMs ha estat massa comercial. Esperava més detall tècnic i menys pitch de producte."),
 ("comment", "sample:020", "Platform Engineer", "en", "2026-09-23T09:55:00Z", 12, 0, 0,
  "Agree, felt like a sponsor demo. The open-source track in room 2 was much better."),
 ("post", None, "Founder, climate-tech", "en", "2026-09-23T10:30:00Z", 219, 19, 8,
  "Loved the session on AI for the blue economy with the Port of Barcelona. Real use cases on port operations, not slides about the future."),
 ("post", None, "Sales lead, cloud provider", "en", "2026-09-23T11:15:00Z", 58, 13, 2,
  "Busy morning at our booth. If you asked about the GPU credits programme for startups, drop a comment and I will follow up personally."),
 ("comment", "sample:023", "Co-founder, seed startup", "en", "2026-09-23T11:40:00Z", 3, 1, 0,
  "Interested in the startup credits. We are training small vision models for manufacturing QA."),
 ("post", None, "UX researcher", "en", "2026-09-23T12:30:00Z", 47, 8, 0,
  "Accessibility at the venue could be better: no captions on the main stage screens and the signage to room 4 was confusing."),
 ("post", None, "Directora de operaciones", "es", "2026-09-23T13:45:00Z", 102, 9, 3,
  "Hemos venido con todo el equipo y volvemos con tres pilotos concretos. Eso es lo que diferencia este evento: IA aplicada, no teoría."),
 ("post", None, "Research scientist", "en", "2026-09-23T15:20:00Z", 178, 21, 7,
  "Best panel of the summit: open weights vs closed models for enterprise. Honest disagreement on stage, no marketing. More of this please."),
 ("comment", "sample:027", "ML Ops Engineer", "en", "2026-09-23T15:40:00Z", 19, 0, 0,
  "That panel was the highlight for me too. Is there a recording?"),
 ("post", None, "Hackathon participant", "en", "2026-09-23T18:00:00Z", 511, 44, 25,
  "Our team won second place at the AI Summit hackathon with an agent that reroutes port trucks in real time. 24 hours, zero sleep, zero regrets."),
 ("post", None, "Freelance data engineer", "ca", "2026-09-23T19:10:00Z", 36, 4, 0,
  "Cues interminables al guarda-roba en sortir. Detall petit, però després de dos dies llargs es nota."),
 ("post", None, "VP Engineering", "en", "2026-09-24T08:30:00Z", 245, 23, 11,
  "Recap of AI Summit Barcelona: agents are moving to production, evals are a first-class discipline, and Barcelona is now firmly on the European AI map. Already booked for 2027."),
 ("comment", "sample:031", "Talent partner", "en", "2026-09-24T09:00:00Z", 8, 0, 0,
  "Great summary. Would add: the hiring market for applied ML in Barcelona is hot right now."),
 ("post", None, "Periodista tecnológica", "es", "2026-09-24T10:15:00Z", 93, 6, 4,
  "Crónica del AI Summit Barcelona: más de 10.000 asistentes en el WTC y un ambiente muy centrado en casos reales. El reto para 2027 será la logística."),
 ("post", None, "Community manager", "en", "2026-09-24T14:00:00Z", 67, 15, 2,
  "Where can we find the slides and recordings from AI Summit Barcelona? Several speakers promised to share but nothing on the website yet."),
 ("post", None, "Founder, edtech", "en", "2026-09-25T09:20:00Z", 84, 7, 1,
  "Two days later and still processing everything from #AISummitBCN. Met our next pilot customer at a side event in 22@."),
 ("post", None, "Data & AI lead, bank", "en", "2026-09-25T11:45:00Z", 52, 3, 0,
  "Mixed feelings about the summit: excellent technical tracks, but too crowded and hard to find quiet spots for real conversations."),
]

if SEED_SAMPLE:
    import json
    with open(f"{VOLUME_PATH}/sample_linkedin_posts.json", "w") as f:
        for i, (typ, parent, headline, lang, ts, rx, cm, rp, text) in enumerate(_S, start=1):
            f.write(json.dumps({"item_id": f"sample:{i:03d}", "item_type": typ, "parent_id": parent,
                                "author": f"sample-member-{i:03d}", "author_headline": headline, "text": text,
                                "lang": lang, "created_at": ts, "reactions": rx, "comments": cm, "reposts": rp,
                                "url": None, "source": "sample"}, ensure_ascii=False) + "\n")
    print(f"Seeded {len(_S)} sample items -> {VOLUME_PATH}/sample_linkedin_posts.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live connection test — pull 10 recent items
# MAGIC API mode: the Page's latest posts plus comments. Volume mode: 10 items from the drop folder.
# MAGIC Either way they land in the temp view `live_sample` for the AI tests below.

# COMMAND ----------

_mode = resolve_source_mode()
if _mode == "api":
    try:
        _rows = li_fetch(get_linkedin_token(), LI_ORG_URN, max_posts=5, max_comments=5)[:10]
    except Exception as e:
        raise RuntimeError(f"{e}\nIf this is a 401/403 check the token scopes and that you are a Page admin; "
                           "if the host is unreachable, outbound internet to api.linkedin.com is blocked in this "
                           "workspace — use Option B/C (SOURCE_MODE='volume').")
    sample = spark.createDataFrame(_rows, BRONZE_SCHEMA)
else:
    sample = spark.read.json(f"{VOLUME_PATH}/*.json").limit(10)
sample.select("item_id", "item_type", "lang", "text").createOrReplaceTempView("live_sample")
display(spark.table("live_sample"))
print(f"Mode = {_mode}. If empty: in API mode check LI_ORG_URN / LOOKBACK_FROM; in volume mode seed Option C.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the AI functions on the real posts
# MAGIC Sentiment, topic, AI theme, entities and English translation — applied to the items we just pulled.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   text,
# MAGIC   ai_analyze_sentiment(text)                                                              AS sentiment,
# MAGIC   ai_classify(text, ARRAY('session praise','session complaint','logistics','networking & hiring','sponsor & lead signal','general chatter')) AS topic,
# MAGIC   ai_classify(text, ARRAY('agents','genai & llms','data & infrastructure','ethics & regulation','industry use cases','startups & funding','talent & careers','other')) AS theme,
# MAGIC   ai_extract(text, ARRAY('speaker','company','location'))                                 AS entities,
# MAGIC   ai_translate(text, 'en')                                                                AS english
# MAGIC FROM live_sample;

# COMMAND ----------

# MAGIC %md
# MAGIC ## A custom task with ai_query — beyond the managed built-ins
# MAGIC The functions above are fully managed. For anything custom we use `ai_query(endpoint, prompt)`,
# MAGIC passing a Databricks-hosted model directly — here `system.ai.claude-opus-4-7`. We ask the model to draft
# MAGIC a one-line action for the summit team for each post — a generative task with no built-in function. The
# MAGIC same call runs across every row, so this is **batch inference** on our data.

# COMMAND ----------

_PROMPT = ('You are the community and operations lead for AI Summit Barcelona 2026 (22-23 September, World Trade '
           'Center Barcelona). Reply with ONLY compact JSON with two keys: "bucket" (one of: amplify, fix_ops, '
           'follow_up, content, monitor) and "action" (one short imperative action, max 12 words). '
           'Example. Post: "Registration queue at the WTC took 45 minutes and the main auditorium closed its doors '
           'before the keynote." Response: {"bucket": "fix_ops", "action": "Add badge desks and an overflow room '
           'with live stream"}. Now classify this LinkedIn post: ')
assert "'" not in _PROMPT, "No single quotes in the prompt (it is embedded in a SQL string literal)."
display(spark.sql(f"""
SELECT text,
  ai_query('databricks-meta-llama-3-3-70b-instruct', '{_PROMPT}' || text) AS action_json
FROM live_sample
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC The connection works, the managed AI functions run on real LinkedIn posts, and `ai_query` adds a custom
# MAGIC JSON column. Next: **02_Pipeline** builds Bronze -> Silver -> Gold.