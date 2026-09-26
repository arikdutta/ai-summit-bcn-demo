"""AI Summit BCN 2026 - LinkedIn sentiment (single-page Databricks App).

Reads workspace.ai_summit.gold_linkedin_metrics. Every panel is tied back to a Databricks AI function: sentiment ->
ai_analyze_sentiment, topic & theme -> ai_classify, speaker/company/location -> ai_extract, English -> ai_translate,
and a custom suggested-action column -> ai_query. Genie in the sidebar. Auto-refreshes every 15s. Barcelona time.
"""
import os, time, base64, html as _html, datetime as dt
import pandas as pd, altair as alt, streamlit as st
from databricks.sdk import WorkspaceClient

CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "ai_summit")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
GENIE_TITLE = os.getenv("GENIE_TITLE", "AI Summit BCN - LinkedIn")
FQ = f"{CATALOG}.{SCHEMA}"
T = f"{FQ}.gold_linkedin_metrics"
TZ = "Europe/Madrid"

# Sentiment palette (explicit: green = positive, red = negative).
C = {"positive": "#16a34a", "negative": "#dc2626", "neutral": "#64748b", "mixed": "#d97706"}
RGB = {"positive": [22, 163, 74], "negative": [220, 38, 38], "neutral": [100, 116, 139], "mixed": [217, 119, 6]}
DOM, RNG = list(C), list(C.values())

# Barcelona AI Week places -> (lat, lon). Reference data used to place the map; counts come only from real
# posts. Matched case-insensitively as substrings of the extracted location (accents stripped).
GEO = {
    "world trade center": (41.3716, 2.1797), "wtc": (41.3716, 2.1797), "port of barcelona": (41.3716, 2.1797),
    "port de barcelona": (41.3716, 2.1797), "puerto de barcelona": (41.3716, 2.1797),
    "port vell": (41.3758, 2.1830), "pier01": (41.3810, 2.1860), "palau de mar": (41.3810, 2.1860),
    "barceloneta": (41.3797, 2.1892), "drassanes": (41.3755, 2.1760), "montjuic": (41.3641, 2.1585),
    "poblenou": (41.4035, 2.1960), "22@": (41.4020, 2.1940), "eixample": (41.3910, 2.1649),
    "gracia": (41.4036, 2.1560), "placa catalunya": (41.3870, 2.1700), "ciutat vella": (41.3833, 2.1777),
    "el born": (41.3849, 2.1817), "ciutadella": (41.3880, 2.1870), "sagrada familia": (41.4036, 2.1744),
    "fira": (41.3553, 2.1277), "diagonal": (41.3947, 2.1490), "upc": (41.3890, 2.1130),
    "barcelona": (41.3874, 2.1686),
}


def _fold(s):
    import unicodedata
    return "".join(ch for ch in unicodedata.normalize("NFKD", s.lower()) if not unicodedata.combining(ch))


st.set_page_config(page_title="AI Summit BCN - LinkedIn Pulse", page_icon="\U0001f916", layout="wide",
                   initial_sidebar_state="expanded")

# ---------- styling: summit banner, clean cards ----------
st.markdown("""
<style>
.block-container{padding-top:1.1rem;max-width:1400px}
.av-banner{background:linear-gradient(105deg,#0a66c2 0%,#4338ca 45%,#7c3aed 75%,#06b6d4 100%);
  border-radius:18px;padding:20px 26px;color:#fff;box-shadow:0 8px 26px rgba(0,0,0,.18);
  display:flex;align-items:center;gap:22px;flex-wrap:wrap}
.av-banner h1{margin:0;font-size:30px;letter-spacing:.3px;font-weight:800;line-height:1.05;
  text-shadow:0 1px 8px rgba(0,0,0,.25)}
.av-banner .sub{margin-top:4px;font-size:14px;font-weight:500;opacity:.97}
.av-tags{margin-top:8px;display:flex;gap:7px;flex-wrap:wrap}
.av-tag{background:rgba(255,255,255,.2);backdrop-filter:blur(4px);border:1px solid rgba(255,255,255,.35);
  padding:2px 11px;border-radius:999px;font-size:12px;font-weight:600}
.av-badge{margin-left:auto;text-align:center;background:rgba(0,0,0,.2);border-radius:14px;padding:10px 16px}
.av-badge b{display:block;font-size:22px;font-weight:800;line-height:1}
.av-badge span{font-size:11px;text-transform:uppercase;letter-spacing:1px;opacity:.9}
.kpi{border:1px solid rgba(128,128,128,.2);border-radius:14px;padding:14px 16px;background:var(--kpi-bg,#ffffff)}
.kpi .v{font-size:26px;font-weight:800;line-height:1.1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.kpi .l{font-size:11px;text-transform:uppercase;letter-spacing:.7px;color:#64748b;margin-top:4px}
.sect{font-weight:700;font-size:15px;margin:6px 0 2px}
.sect small{font-weight:500;color:#64748b}
@media (prefers-color-scheme:dark){ :root{ --kpi-bg:#12161d } }
.twt{border-collapse:collapse;width:100%;font-size:13px}
.twt th,.twt td{border-bottom:1px solid rgba(128,128,128,.18);padding:8px 10px;text-align:left;vertical-align:top}
.twt th{position:sticky;top:0;background:var(--th-bg,#f1f5f9);font-size:11px;color:#475569}
@media (prefers-color-scheme:dark){ .twt th{--th-bg:#12161d;color:#94a3b8} }
.twt .w{white-space:normal;word-break:break-word;min-width:260px}.twt .nw{white-space:nowrap}
.pill{color:#fff;padding:2px 9px;border-radius:999px;font-size:11px;font-weight:700}
.typ{border:1px solid rgba(128,128,128,.4);padding:1px 7px;border-radius:6px;font-size:11px}
.sample{background:#fef3c7;color:#92400e;border:1px solid #fcd34d;border-radius:10px;padding:6px 12px;
  font-size:13px;margin:8px 0}
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def wc():
    return WorkspaceClient()


@st.cache_resource
def warehouse_id():
    if WAREHOUSE_ID:
        return WAREHOUSE_ID
    whs = list(wc().warehouses.list())
    pick = (next((x for x in whs if "Serverless Starter" in (x.name or "")), None)
            or next((x for x in whs if x.enable_serverless_compute), None) or (whs[0] if whs else None))
    return pick.id if pick else ""


@st.cache_resource
def genie_space_id():
    if GENIE_SPACE_ID:
        return GENIE_SPACE_ID
    try:
        spaces = wc().api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
        return next((s["space_id"] for s in spaces if s.get("title") == GENIE_TITLE), "")
    except Exception:
        return ""


@st.cache_data(ttl=12, show_spinner=False)
def q(sql):
    w = wc()
    r = w.statement_execution.execute_statement(warehouse_id=warehouse_id(), statement=sql, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(getattr(r.status, "error", "query failed"))
    return pd.DataFrame(r.result.data_array or [], columns=[c.name for c in r.manifest.schema.columns])


def genie_ask(question):
    w = wc(); base = f"/api/2.0/genie/spaces/{genie_space_id()}"; cid = st.session_state.get("cid")
    r = (w.api_client.do("POST", f"{base}/conversations/{cid}/messages", body={"content": question}) if cid
         else w.api_client.do("POST", f"{base}/start-conversation", body={"content": question}))
    st.session_state["cid"] = r.get("conversation_id", cid); mid = r.get("message_id") or r.get("id"); m = {}
    for _ in range(40):
        m = w.api_client.do("GET", f"{base}/conversations/{st.session_state['cid']}/messages/{mid}")
        if m.get("status") == "COMPLETED":
            break
        if m.get("status") in ("FAILED", "CANCELLED"):
            return {"text": "Genie could not answer that one.", "df": None}
        time.sleep(2)
    text = None; df = None
    for a in m.get("attachments", []):
        if a.get("text"):
            text = a["text"].get("content")
        if a.get("query"):
            try:
                sr = w.api_client.do("GET", f"{base}/conversations/{st.session_state['cid']}/messages/{mid}"
                                     f"/attachments/{a['attachment_id']}/query-result")["statement_response"]
                df = pd.DataFrame(sr["result"].get("data_array") or [],
                                  columns=[c["name"] for c in sr["manifest"]["schema"]["columns"]])
            except Exception:
                df = None
    return {"text": text, "df": df}


# ---------- sidebar: Genie planner ----------
with st.sidebar:
    st.markdown("### \U0001f9de Genie Planner")
    st.caption("AI/BI Genie \u2014 ask in plain language, it writes the SQL.")
    st.session_state.setdefault("hist", [])
    with st.form("g", clear_on_submit=True):
        gq = st.text_area("Ask", height=72, label_visibility="collapsed",
                          placeholder="Which sponsor or hiring leads should we follow up?")
        ask = st.form_submit_button("Ask Genie", width="stretch", type="primary")
    if ask and gq:
        if not genie_space_id():
            st.warning(f"No Genie space found (set GENIE_SPACE_ID or create '{GENIE_TITLE}' in Module 04).")
        else:
            with st.spinner("Genie is thinking..."):
                st.session_state["hist"].append((gq, genie_ask(gq)))
    for qt, a in reversed(st.session_state["hist"]):
        st.markdown(f"**{qt}**")
        if a.get("text"):
            st.markdown(a["text"])
        if a.get("df") is not None and not a["df"].empty:
            st.dataframe(a["df"], width="stretch", hide_index=True)
        st.divider()

# ---------- header banner ----------
try:
    logo = base64.b64encode(open(os.path.join(os.path.dirname(__file__), "summit_logo.png"), "rb").read()).decode()
    logo_html = f'<img src="data:image/png;base64,{logo}" style="height:64px;border-radius:10px;background:#fff;padding:6px"/>'
except Exception:
    logo_html = ""
st.markdown(f"""
<div class="av-banner">
  {logo_html}
  <div>
    <h1>AI Summit BCN 2026</h1>
    <div class="sub">World Trade Center Barcelona \u00b7 22\u201323 Sept \u2014 reading the LinkedIn conversation</div>
    <div class="av-tags"><span class="av-tag">#AISummitBCN</span><span class="av-tag">#AIWeek</span>
      <span class="av-tag">#BeyondTheory</span><span class="av-tag">LinkedIn</span></div>
  </div>
  <div class="av-badge"><span>AI Summit</span><b>2026</b><span>Barcelona</span></div>
</div>
""", unsafe_allow_html=True)
st.caption("Sentiment via ai_analyze_sentiment \u00b7 Topic & Theme via ai_classify \u00b7 Speaker, Company & Location "
           "via ai_extract \u00b7 English via ai_translate \u00b7 Action via ai_query (custom) \u00b7 times in "
           "Barcelona (CET/CEST)")


@st.fragment(run_every=15)
def live():
    st.caption(f"\U0001f534 live \u00b7 refreshed {dt.datetime.now(dt.timezone.utc).astimezone():%H:%M:%S} "
               f"\u00b7 auto every 15s")

    # ----- KPIs -----
    try:
        s = q(f"SELECT sentiment, count(*) items FROM {T} GROUP BY sentiment")
        s["items"] = pd.to_numeric(s["items"])
        total = int(s["items"].sum())
    except Exception:
        s = pd.DataFrame(columns=["sentiment", "items"]); total = 0

    if total == 0:
        st.info("No LinkedIn posts in the Gold table yet. Once the pipeline ingests posts and comments about the "
                "summit, the panels below light up automatically (refreshes every 15s).")
        return

    try:
        src = q(f"SELECT count_if(source = 'sample') n FROM {T}")
        if int(src["n"].iloc[0] or 0) > 0:
            st.markdown("<div class='sample'>\u26a0\ufe0f Includes the fictional <b>sample</b> dataset from "
                        "Module 01 \u2014 remove it from the Volume before presenting real results.</div>",
                        unsafe_allow_html=True)
    except Exception:
        pass

    pos = int(s.loc[s.sentiment == "positive", "items"].sum())
    neg = int(s.loc[s.sentiment == "negative", "items"].sum())
    try:
        th = q(f"""SELECT theme FROM {T} WHERE theme IS NOT NULL AND theme <> 'other'
                   GROUP BY theme ORDER BY sum(CASE WHEN sentiment='positive' THEN 1 ELSE 0 END) DESC,
                   count(*) DESC LIMIT 1""")
        top_theme = th["theme"].iloc[0] if not th.empty else "\u2014"
    except Exception:
        top_theme = "\u2014"

    k = st.columns(4)
    for col, val, lab, color in [
        (k[0], f"{total}", "Posts & comments analysed", "#0a66c2"),
        (k[1], f"{round(100*pos/total)}%", "Positive", C["positive"]),
        (k[2], f"{round(100*neg/total)}%", "Negative", C["negative"]),
        (k[3], top_theme, "Most-loved AI theme", "#4338ca")]:
        col.markdown(f"<div class='kpi'><div class='v' style='color:{color}'>{_html.escape(str(val))}</div>"
                     f"<div class='l'>{lab}</div></div>", unsafe_allow_html=True)

    st.write("")
    a, b = st.columns([1, 1.4])
    with a:
        st.markdown("<div class='sect'>Sentiment mix <small>ai_analyze_sentiment</small></div>", unsafe_allow_html=True)
        st.altair_chart(
            alt.Chart(s).mark_arc(innerRadius=58).encode(
                theta="items:Q",
                color=alt.Color("sentiment:N", scale=alt.Scale(domain=DOM, range=RNG),
                                legend=alt.Legend(orient="bottom", title=None)),
                tooltip=["sentiment", "items"]).properties(height=280),
            width="stretch")
    with b:
        st.markdown("<div class='sect'>Sentiment over time <small>cumulative per hour, Barcelona time</small></div>",
                    unsafe_allow_html=True)
        try:
            t = q(f"""SELECT ts, sentiment,
                             sum(items) OVER (PARTITION BY sentiment ORDER BY ts
                                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum
                      FROM (SELECT date_trunc('HOUR', from_utc_timestamp(created_at,'{TZ}')) ts,
                                   sentiment, count(*) items
                            FROM {T} GROUP BY 1,2)
                      ORDER BY ts""")
            t["cum"] = pd.to_numeric(t["cum"]); t["ts"] = pd.to_datetime(t["ts"])
            st.altair_chart(
                alt.Chart(t).mark_line(point=True, interpolate="monotone").encode(
                    x=alt.X("ts:T", title=None), y=alt.Y("cum:Q", title="posts & comments (cumulative)"),
                    color=alt.Color("sentiment:N", scale=alt.Scale(domain=DOM, range=RNG),
                                    legend=alt.Legend(orient="bottom", title=None)),
                    tooltip=["ts:T", "sentiment", "cum"]).properties(height=280),
                width="stretch")
        except Exception as e:
            st.info(f"No timeline yet. ({e})")

    # ----- AI themes + actions -----
    c1, c2 = st.columns([1.4, 1])
    with c1:
        st.markdown("<div class='sect'>What the AI crowd talked about <small>theme via ai_classify \u00b7 "
                    "colour = mood</small></div>", unsafe_allow_html=True)
        try:
            tm = q(f"SELECT theme, sentiment, count(*) items FROM {T} WHERE theme IS NOT NULL GROUP BY 1,2")
            tm["items"] = pd.to_numeric(tm["items"])
            st.altair_chart(
                alt.Chart(tm).mark_bar().encode(
                    x=alt.X("sum(items):Q", title="posts & comments"),
                    y=alt.Y("theme:N", sort="-x", title=None),
                    color=alt.Color("sentiment:N", scale=alt.Scale(domain=DOM, range=RNG),
                                    legend=alt.Legend(orient="bottom", title=None)),
                    tooltip=["theme", "sentiment", "items"]).properties(height=280),
                width="stretch")
        except Exception as e:
            st.info(f"No themes yet. ({e})")
    with c2:
        st.markdown("<div class='sect'>Recommended actions <small>ai_query (custom)</small></div>",
                    unsafe_allow_html=True)
        try:
            ac = q(f"SELECT action_bucket, count(*) items FROM {T} WHERE action_bucket IS NOT NULL GROUP BY 1")
            ac["items"] = pd.to_numeric(ac["items"])
            st.altair_chart(
                alt.Chart(ac).mark_bar(color="#4338ca").encode(
                    x=alt.X("items:Q", title="posts & comments"),
                    y=alt.Y("action_bucket:N", sort="-x", title=None),
                    tooltip=["action_bucket", "items"]).properties(height=280),
                width="stretch")
        except Exception as e:
            st.info(f"No actions yet. ({e})")

    # ----- full-text LinkedIn wall -----
    st.markdown("<div class='sect'>Live LinkedIn wall <small>posts &amp; comments \u00b7 each column tied to an AI "
                "function</small></div>", unsafe_allow_html=True)
    try:
        tbl = q(f"""SELECT from_utc_timestamp(created_at,'{TZ}') AS created_at, item_type, sentiment, topic, theme,
                           speaker, company, engagement, action_bucket, recommended_action, text_en,
                           original_text, url
                    FROM {T} ORDER BY created_at DESC LIMIT 200""")

        def e(x, n=None):
            x = "" if x is None else str(x)
            return _html.escape(x if n is None or len(x) <= n else x[:n] + "\u2026")
        rows = ""
        for _, r in tbl.iterrows():
            ts = pd.to_datetime(r["created_at"]).strftime("%d %b %H:%M")
            col = C.get(str(r["sentiment"]), C["neutral"])
            link = (f" <a href='{e(r['url'])}' target='_blank'>\u2197</a>"
                    if r["url"] and str(r["url"]).startswith("https://") else "")
            rows += (f"<tr><td class=nw>{ts}</td><td><span class='typ'>{e(r['item_type'])}</span></td>"
                     f"<td><span class='pill' style='background:{col}'>{e(r['sentiment'])}</span></td>"
                     f"<td class=nw>{e(r['topic'])}</td><td class=nw>{e(r['theme'])}</td>"
                     f"<td>{e(r['speaker'])}</td><td>{e(r['company'])}</td><td>{e(r['engagement'])}</td>"
                     f"<td class=nw>{e(r['action_bucket'])}</td><td class=w>{e(r['recommended_action'])}</td>"
                     f"<td class=w>{e(r['text_en'], 400)}{link}</td><td class=w>{e(r['original_text'], 400)}</td></tr>")
        st.markdown(
            "<div style='max-height:460px;overflow:auto;border:1px solid rgba(128,128,128,.2);border-radius:10px'>"
            "<table class=twt><thead><tr>"
            "<th>Time<br>Barcelona</th><th>Type</th><th>Sentiment<br>ai_analyze_sentiment</th>"
            "<th>Topic<br>ai_classify</th><th>Theme<br>ai_classify</th>"
            "<th>Speaker<br>ai_extract</th><th>Company<br>ai_extract</th><th>Engagement</th>"
            "<th>Bucket<br>ai_query (custom)</th><th>Suggested action<br>ai_query (custom)</th>"
            "<th>English<br>ai_translate</th><th>Original post</th></tr></thead>"
            f"<tbody>{rows}</tbody></table></div>", unsafe_allow_html=True)
    except Exception as ex:
        st.info(f"No posts yet. ({ex})")

    # ----- AI Week map -----
    st.write("")
    st.markdown("<div class='sect'>AI Week map <small>bubble size = post volume, colour = mood \u00b7 location via "
                "ai_extract</small></div>", unsafe_allow_html=True)
    try:
        loc = q(f"""SELECT lower(trim(location)) location, sentiment, count(*) n
                    FROM {T}
                    WHERE location IS NOT NULL AND trim(location)<>'' AND lower(location)<>'unknown'
                    GROUP BY 1,2""")
        loc["n"] = pd.to_numeric(loc["n"])
        agg = {}  # (lat,lon) -> {"total":int, "byS":{sentiment:count}, "name":str}
        for _, r in loc.iterrows():
            name = str(r["location"])
            hit = next((v for k, v in GEO.items() if k in _fold(name)), None)
            if not hit:
                continue
            d = agg.setdefault(hit, {"total": 0, "byS": {}, "name": name})
            d["total"] += int(r["n"])
            d["byS"][r["sentiment"]] = d["byS"].get(r["sentiment"], 0) + int(r["n"])
        pts = []
        for (lat, lon), d in agg.items():
            dom = max(d["byS"], key=d["byS"].get)
            pts.append({"lat": lat, "lon": lon, "place": d["name"].title(), "items": d["total"],
                        "mood": dom, "color": RGB.get(dom, RGB["neutral"]), "radius": 120 + d["total"] * 55})
        if not pts:
            st.info("No mappable locations extracted yet \u2014 the map fills in as posts mention Barcelona places "
                    "(World Trade Center, Port Vell, Poblenou, 22@ ...).")
        else:
            import pydeck as pdk
            mp = pd.DataFrame(pts)
            layer = pdk.Layer("ScatterplotLayer", data=mp, get_position="[lon, lat]",
                              get_radius="radius", get_fill_color="[color[0], color[1], color[2], 150]",
                              get_line_color=[255, 255, 255], line_width_min_pixels=1,
                              pickable=True, stroked=True, radius_min_pixels=6, radius_max_pixels=90)
            view = pdk.ViewState(latitude=41.382, longitude=2.175, zoom=12.2, pitch=35)
            deck = pdk.Deck(layers=[layer], initial_view_state=view, map_provider="carto", map_style="light",
                            tooltip={"html": "<b>{place}</b><br/>{items} posts \u00b7 mood: {mood}"})
            st.pydeck_chart(deck, width="stretch")
    except Exception as ex:
        # Guaranteed fallback: posts-by-location bar chart (no map tiles needed).
        try:
            bar = q(f"""SELECT location, count(*) items FROM {T}
                        WHERE location IS NOT NULL AND trim(location)<>'' AND lower(location)<>'unknown'
                        GROUP BY location ORDER BY items DESC LIMIT 12""")
            bar["items"] = pd.to_numeric(bar["items"])
            st.altair_chart(
                alt.Chart(bar).mark_bar(color="#0a66c2").encode(
                    x=alt.X("items:Q", title="posts & comments"),
                    y=alt.Y("location:N", sort="-x", title=None),
                    tooltip=["location", "items"]).properties(height=320),
                width="stretch")
        except Exception:
            st.info(f"No location data yet. ({ex})")


live()
