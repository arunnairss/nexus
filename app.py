"""
app.py — NEXUS Dashboard · Real-Time User Activity Tracker
Run: streamlit run app.py

UI APPROACH: All visual components are rendered as raw HTML via st.markdown()
so CSS actually applies — no Streamlit component wrapper interference.
"""

import sys, time
sys.path.insert(0, "modules")

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

from kafka_simulator      import EventProducer, kafka_bus, TOPIC
from spark_processor      import SparkProcessor
from cassandra_simulator  import CassandraStore

# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NEXUS · Tracker",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",   # collapsed so we own ALL the space
)

# ─────────────────────────────────────────────────────────────
#  INJECT FONTS + FULL PAGE RESET
#  Everything rendered after this inherits our design tokens.
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=JetBrains+Mono:wght@400;600&family=Outfit:wght@300;400;500;600&display=swap');

/* ── hard reset ── */
html, body { margin:0; padding:0; }

/* ── kill every Streamlit wrapper background ── */
.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
[data-testid="block-container"],
.main, section.main,
[data-testid="stVerticalBlock"],
[data-testid="stHorizontalBlock"],
div[class*="css"] {
    background: transparent !important;
}

/* ── THE actual page background ── */
.stApp {
    background:
        radial-gradient(ellipse 90% 60% at 10% 0%,   rgba(0,212,188,0.09) 0%, transparent 50%),
        radial-gradient(ellipse 70% 50% at 90% 100%,  rgba(220,38,127,0.08) 0%, transparent 50%),
        radial-gradient(ellipse 50% 40% at 50% 50%,   rgba(139,92,246,0.04) 0%, transparent 60%),
        #060812 !important;
    font-family: 'Outfit', sans-serif;
    color: #e2e8f0;
}

/* ── grid overlay ── */
.stApp::before {
    content: '';
    position: fixed; inset: 0; pointer-events: none; z-index: 0;
    background-image:
        linear-gradient(rgba(0,212,188,0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0,212,188,0.03) 1px, transparent 1px);
    background-size: 44px 44px;
}

/* ── remove ALL padding Streamlit adds ── */
.block-container,
[data-testid="block-container"] {
    padding: 0 !important;
    max-width: 100% !important;
}

/* ── hide chrome ── */
#MainMenu, footer, header,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="stStatusWidget"],
.viewerBadge_container__1QSob { display: none !important; }

/* ── sidebar ── */
[data-testid="stSidebar"] {
    background: #080b16 !important;
    border-right: 1px solid rgba(0,212,188,0.12) !important;
}

/* ── plotly container reset ── */
.stPlotlyChart > div { background: transparent !important; }
.js-plotly-plot, .plotly, .plot-container { background: transparent !important; }

/* ── dataframe: make it dark ── */
[data-testid="stDataFrame"] iframe,
[data-testid="stDataFrame"] > div {
    background: #0a0e1a !important;
    border-radius: 12px !important;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
#  PIPELINE INIT
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def init_pipeline():
    store     = CassandraStore()
    processor = SparkProcessor(store)
    producer  = EventProducer(rate_per_sec=8)
    processor.start(); producer.start()
    return producer, processor, store

producer, processor, store = init_pipeline()

# ─────────────────────────────────────────────────────────────
#  SIDEBAR  (collapsed by default but still functional)
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:20px 0 24px;font-family:'Outfit',sans-serif">
      <div style="font-family:'Syne',sans-serif;font-weight:800;font-size:1.4rem;
                  color:#f1f5f9;letter-spacing:-0.03em">⚡ NEXUS</div>
      <div style="font-size:0.62rem;color:#334155;letter-spacing:0.14em;
                  font-family:'JetBrains Mono',monospace;margin-top:4px">
        ACTIVITY TRACKER
      </div>
    </div>""", unsafe_allow_html=True)

    rate    = st.slider("Events / sec", 1, 30, 8)
    refresh = st.slider("Refresh (sec)", 1, 10, 3)
    producer.rate = rate

# ─────────────────────────────────────────────────────────────
#  LIVE DATA
# ─────────────────────────────────────────────────────────────
now     = datetime.now()
sp      = processor.stats()
summary = store.latest_summary()
events  = store.get_recent_events(50)
series  = store.event_rate_series()

# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────
def card(content: str, pad="24px 28px", extra_style="") -> str:
    return f"""
    <div style="background:linear-gradient(135deg,rgba(14,18,35,0.95),rgba(10,13,25,0.95));
                border:1px solid rgba(255,255,255,0.07);border-radius:16px;
                padding:{pad};position:relative;overflow:hidden;{extra_style}">
        <div style="position:absolute;top:0;left:0;right:0;height:1px;
                    background:linear-gradient(90deg,rgba(0,212,188,0.6),rgba(0,212,188,0.0))">
        </div>
        {content}
    </div>"""

def badge(text, color="#00d4bc"):
    return f"""<span style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;
        padding:3px 10px;border-radius:20px;letter-spacing:0.1em;
        background:{color}18;color:{color};border:1px solid {color}35">{text}</span>"""

def section_title(main, sub=""):
    sub_html = f"""<span style="font-family:'JetBrains Mono',monospace;font-weight:400;
        font-size:0.6rem;color:#2d3a52;letter-spacing:0.12em;margin-left:10px">{sub}</span>""" if sub else ""
    return f"""<div style="font-family:'Syne',sans-serif;font-weight:700;font-size:0.95rem;
        letter-spacing:-0.02em;color:#f1f5f9;margin-bottom:16px;display:flex;
        align-items:center">{main}{sub_html}</div>"""

def plotly_base(h=230):
    return dict(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="JetBrains Mono, monospace", color="#2d3a52", size=10),
        margin=dict(l=2, r=8, t=8, b=2), height=h,
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", linecolor="rgba(255,255,255,0.06)",
                   tickfont=dict(color="#2d3a52", size=9), zeroline=False),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", linecolor="rgba(255,255,255,0.06)",
                   tickfont=dict(color="#4a5568", size=9), zeroline=False),
    )

# ─────────────────────────────────────────────────────────────
#  OUTER WRAPPER  — gives consistent padding
# ─────────────────────────────────────────────────────────────
st.markdown("""
<div style="padding:32px 40px 0;position:relative;z-index:1">
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════
st.markdown(f"""
<div style="display:flex;align-items:flex-end;justify-content:space-between;
            margin-bottom:32px;flex-wrap:wrap;gap:16px">
  <div>
    <div style="font-family:'JetBrains Mono',monospace;font-size:0.62rem;
                color:#2d3a52;letter-spacing:0.18em;margin-bottom:8px">
      NEXUS &nbsp;/&nbsp; REAL-TIME ANALYTICS
    </div>
    <h1 style="font-family:'Syne',sans-serif;font-weight:800;margin:0;
               font-size:clamp(1.8rem,3.5vw,2.6rem);letter-spacing:-0.04em;
               color:#f1f5f9;line-height:1.05">
      User Activity<br>
      <span style="background:linear-gradient(90deg,#00d4bc,#7c3aed);
                   -webkit-background-clip:text;-webkit-text-fill-color:transparent;
                   background-clip:text">
        Tracking System
      </span>
    </h1>
    <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;
                color:#2d3a52;margin-top:10px;letter-spacing:0.05em">
      Kafka &nbsp;→&nbsp; Spark &nbsp;→&nbsp; Cassandra
      &nbsp;&nbsp;·&nbsp;&nbsp;
      {now.strftime('%H:%M:%S')}
      &nbsp;&nbsp;·&nbsp;&nbsp;
      Queue depth: {kafka_bus.qsize(TOPIC)}
    </div>
  </div>
  <div style="display:flex;flex-direction:column;align-items:flex-end;gap:10px">
    <div style="display:inline-flex;align-items:center;gap:10px;
                background:rgba(0,212,188,0.07);
                border:1px solid rgba(0,212,188,0.2);
                border-radius:30px;padding:10px 20px">
      <span style="width:8px;height:8px;border-radius:50%;background:#00d4bc;
                   box-shadow:0 0 10px #00d4bc;display:inline-block"></span>
      <span style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;
                   color:#00d4bc;letter-spacing:0.08em">
        LIVE · {sp['total_processed']:,} EVENTS
      </span>
    </div>
    <div style="display:flex;gap:8px">
      {badge("KAFKA","#00d4bc")}
      {badge("SPARK","#f59e0b")}
      {badge("CASSANDRA","#dc267f")}
      {badge("PYTHON","#8b5cf6")}
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  KPI CARDS  — pure HTML, guaranteed styling
# ══════════════════════════════════════════════════════════════
def kpi_card(icon, label, value, delta, color, delta_up=True):
    arrow = "▲" if delta_up else "▼"
    delta_color = "#10b981" if delta_up else "#ef4444"
    return f"""
    <div style="background:linear-gradient(135deg,rgba(14,18,35,0.95),rgba(8,11,22,0.98));
                border:1px solid rgba(255,255,255,0.07);border-radius:16px;
                padding:22px 24px;position:relative;overflow:hidden;
                transition:all 0.2s;cursor:default;height:100%">
      <div style="position:absolute;top:0;left:0;right:0;height:2px;
                  background:linear-gradient(90deg,{color},transparent)"></div>
      <div style="position:absolute;top:-16px;right:-16px;width:72px;height:72px;
                  border-radius:50%;background:{color};filter:blur(32px);opacity:0.35"></div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;
                  text-transform:uppercase;letter-spacing:0.14em;
                  color:#2d3a52;margin-bottom:12px">{icon}&nbsp; {label}</div>
      <div style="font-family:'Syne',sans-serif;font-weight:800;font-size:2rem;
                  letter-spacing:-0.04em;color:{color};line-height:1;margin-bottom:8px">
        {value}
      </div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;
                  color:{delta_color}">{arrow} {delta}</div>
    </div>"""

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1: st.markdown(kpi_card("⚡","Events Processed", f"{sp['total_processed']:,}",
    f"+{sp['avg_batch_size']} per batch", "#00d4bc"), unsafe_allow_html=True)
with kpi2: st.markdown(kpi_card("👥","Unique Users", f"{store.active_users():,}",
    f"{summary.get('mobile_pct',0):.0f}% mobile", "#8b5cf6"), unsafe_allow_html=True)
with kpi3: st.markdown(kpi_card("🗄","Cassandra Rows", f"{store.total_rows():,}",
    f"{sp['batches_done']} batches written", "#dc267f"), unsafe_allow_html=True)
with kpi4: st.markdown(kpi_card("📡","Throughput", f"{sp['avg_event_rate']:.1f}/s",
    f"Queue: {kafka_bus.qsize(TOPIC)} msgs", "#f59e0b"), unsafe_allow_html=True)

st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  ROW 1 — Event Rate Chart  +  Device Donut
# ══════════════════════════════════════════════════════════════
r1a, r1b = st.columns([3, 1], gap="medium")

with r1a:
    st.markdown(section_title("Event Rate Over Time", "SPARK MICRO-BATCHES"), unsafe_allow_html=True)
    if series and len(series) >= 2:
        df_s = pd.DataFrame({"b": range(len(series)), "v": series})
        ma5  = df_s["v"].rolling(5, min_periods=1).mean()
        fig  = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_s["b"], y=df_s["v"], fill="tozeroy",
            fillcolor="rgba(0,212,188,0.08)",
            line=dict(color="#00d4bc", width=2.5), mode="lines",
            name="Events/batch",
            hovertemplate="Batch %{x}<br><b>%{y} events</b><extra></extra>"
        ))
        fig.add_trace(go.Scatter(
            x=df_s["b"], y=ma5,
            line=dict(color="#dc267f", width=1.5, dash="dot"),
            mode="lines", name="5-batch avg",
            hovertemplate="Avg: <b>%{y:.1f}</b><extra></extra>"
        ))
        layout = plotly_base(230)
        layout.update(showlegend=True,
            legend=dict(font=dict(family="JetBrains Mono", size=9, color="#4a5568"),
                        bgcolor="rgba(0,0,0,0)", x=1, y=1.05, xanchor="right",
                        orientation="h"))
        fig.update_layout(**layout)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    else:
        st.markdown(card("<p style='color:#2d3a52;font-family:JetBrains Mono,monospace;"
                         "font-size:.8rem;margin:40px 0;text-align:center'>"
                         "⏳ Waiting for first Spark batch…</p>"), unsafe_allow_html=True)

with r1b:
    st.markdown(section_title("Devices"), unsafe_allow_html=True)
    dev = summary.get("device_counts", {})
    if dev:
        fig2 = go.Figure(go.Pie(
            labels=list(dev.keys()), values=list(dev.values()), hole=0.65,
            marker=dict(colors=["#00d4bc","#dc267f","#f59e0b"],
                        line=dict(color="#060812", width=4)),
            textfont=dict(family="JetBrains Mono", size=9),
            hovertemplate="%{label}<br><b>%{value}</b> · %{percent}<extra></extra>"
        ))
        layout2 = plotly_base(230)
        layout2.update(showlegend=True,
            legend=dict(font=dict(family="JetBrains Mono", size=9, color="#4a5568"),
                        bgcolor="rgba(0,0,0,0)"))
        fig2.update_layout(**layout2)
        st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
    else:
        st.markdown("<p style='color:#2d3a52;text-align:center;padding:60px 0;font-size:.8rem'>⏳</p>",
                    unsafe_allow_html=True)

st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  ROW 2 — Top Pages  +  Event Types
# ══════════════════════════════════════════════════════════════
r2a, r2b = st.columns(2, gap="medium")

with r2a:
    st.markdown(section_title("Top Pages", "CUMULATIVE VIEWS"), unsafe_allow_html=True)
    tp = store.top_pages(8)
    if tp:
        df_p = pd.DataFrame(list(tp.items()), columns=["Page","Views"]).sort_values("Views")
        fig3 = go.Figure(go.Bar(
            x=df_p["Views"], y=df_p["Page"], orientation="h",
            marker=dict(
                color=list(range(len(df_p))),
                colorscale=[[0,"#071820"],[0.4,"#014d5e"],[1,"#00d4bc"]],
                line=dict(width=0)
            ),
            text=[f"  {v:,}" for v in df_p["Views"]], textposition="outside",
            textfont=dict(family="JetBrains Mono", size=9, color="#2d3a52"),
            hovertemplate="%{y}<br><b>%{x:,} views</b><extra></extra>"
        ))
        layout3 = plotly_base(260)
        layout3["yaxis"].update(tickfont=dict(family="JetBrains Mono", size=10, color="#8b9cb6"))
        layout3["showlegend"] = False
        fig3.update_layout(**layout3)
        st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})

with r2b:
    st.markdown(section_title("Event Types", "DISTRIBUTION"), unsafe_allow_html=True)
    te = store.top_events(7)
    if te:
        df_e = pd.DataFrame(list(te.items()), columns=["Event","Count"])
        fig4 = go.Figure(go.Bar(
            x=df_e["Event"], y=df_e["Count"],
            marker=dict(
                color=list(range(len(df_e))),
                colorscale=[[0,"#180828"],[0.4,"#6b1256"],[1,"#dc267f"]],
                line=dict(width=0)
            ),
            text=[f"{v:,}" for v in df_e["Count"]], textposition="outside",
            textfont=dict(family="JetBrains Mono", size=9, color="#2d3a52"),
            hovertemplate="%{x}<br><b>%{y:,}</b><extra></extra>"
        ))
        layout4 = plotly_base(260)
        layout4["xaxis"].update(tickfont=dict(family="JetBrains Mono", size=9, color="#8b9cb6"),
                                tickangle=-20)
        layout4["showlegend"] = False
        fig4.update_layout(**layout4)
        st.plotly_chart(fig4, use_container_width=True, config={"displayModeBar": False})

st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  ROW 3 — Secondary KPIs (HTML cards)
# ══════════════════════════════════════════════════════════════
def mini_kpi(label, value, sub, color):
    return f"""
    <div style="background:rgba(12,15,28,0.9);border:1px solid rgba(255,255,255,0.06);
                border-radius:14px;padding:18px 20px;position:relative;overflow:hidden">
      <div style="position:absolute;top:0;left:0;right:0;height:2px;
                  background:linear-gradient(90deg,{color},transparent)"></div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;
                  text-transform:uppercase;letter-spacing:0.12em;
                  color:#2d3a52;margin-bottom:8px">{label}</div>
      <div style="font-family:'Syne',sans-serif;font-weight:800;
                  font-size:1.6rem;color:#f1f5f9;letter-spacing:-0.03em">{value}</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:0.62rem;
                  color:{color};margin-top:4px">{sub}</div>
    </div>"""

m1, m2, m3 = st.columns(3, gap="medium")
with m1: st.markdown(mini_kpi("↩ Bounce Rate",
    f"{summary.get('bounce_pct',0):.1f}%", "< 200ms sessions", "#f59e0b"),
    unsafe_allow_html=True)
with m2: st.markdown(mini_kpi("📱 Mobile Traffic",
    f"{summary.get('mobile_pct',0):.1f}%", "of current batch", "#8b5cf6"),
    unsafe_allow_html=True)
with m3: st.markdown(mini_kpi("🧑 Batch Users",
    f"{summary.get('unique_users',0)}", "unique in last 2s", "#00d4bc"),
    unsafe_allow_html=True)

st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  PIPELINE STATUS BAR  — horizontal strip
# ══════════════════════════════════════════════════════════════
def pipe_step(icon, name, detail, status, color):
    return f"""
    <div style="flex:1;display:flex;flex-direction:column;align-items:center;
                padding:16px 12px;gap:8px;min-width:0">
      <div style="font-size:1.4rem">{icon}</div>
      <div style="font-family:'Outfit',sans-serif;font-weight:600;
                  font-size:0.82rem;color:#cbd5e1;text-align:center">{name}</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;
                  color:#2d3a52;text-align:center">{detail}</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:0.58rem;
                  padding:3px 10px;border-radius:20px;letter-spacing:0.1em;
                  background:{color}18;color:{color};border:1px solid {color}35">
        {status}
      </div>
    </div>"""

def pipe_arrow():
    return """<div style="display:flex;align-items:center;padding:0 4px;color:#1e2d3d;
                          font-size:1.2rem;flex-shrink:0">→</div>"""

st.markdown(f"""
<div style="background:linear-gradient(135deg,rgba(14,18,35,0.95),rgba(8,11,22,0.98));
            border:1px solid rgba(255,255,255,0.07);border-radius:16px;
            position:relative;overflow:hidden;margin-bottom:24px">
  <div style="position:absolute;top:0;left:0;right:0;height:1px;
              background:linear-gradient(90deg,transparent,rgba(0,212,188,0.4),transparent)"></div>
  <div style="padding:8px 16px;border-bottom:1px solid rgba(255,255,255,0.05)">
    <span style="font-family:'JetBrains Mono',monospace;font-size:0.58rem;
                 color:#2d3a52;letter-spacing:0.16em">PIPELINE STATUS</span>
  </div>
  <div style="display:flex;align-items:stretch">
    {pipe_step("🌐","JS Tracker","Browser events","ACTIVE","#10b981")}
    {pipe_arrow()}
    {pipe_step("⚡","Kafka Bus","user-events topic","STREAMING","#00d4bc")}
    {pipe_arrow()}
    {pipe_step("🔥","Spark","Micro-batch 2s","PROCESSING","#f59e0b")}
    {pipe_arrow()}
    {pipe_step("🗄️","Cassandra","3-node · RF=3","HEALTHY","#10b981")}
    {pipe_arrow()}
    {pipe_step("📊","Dashboard","This interface","LIVE","#8b5cf6")}
  </div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  LIVE EVENT STREAM TABLE
# ══════════════════════════════════════════════════════════════
st.markdown(section_title("Live Event Stream", "CASSANDRA · LATEST 20 ROWS"),
            unsafe_allow_html=True)

EVENT_COLORS = {
    "click":       "#00d4bc",
    "purchase":    "#10b981",
    "page_view":   "#8b5cf6",
    "form_submit": "#f59e0b",
    "add_to_cart": "#dc267f",
    "search":      "#60a5fa",
    "logout":      "#94a3b8",
}

if events:
    df_ev   = pd.DataFrame(events)
    keep    = ["user_id","event_type","page_url","device","browser","duration_ms","timestamp"]
    keep    = [c for c in keep if c in df_ev.columns]
    df_show = df_ev[keep].head(20).copy()
    df_show.columns = [c.replace("_"," ").upper() for c in df_show.columns]

    def s_event(v):
        c = EVENT_COLORS.get(str(v).lower(),"#64748b")
        return f"color:{c};font-family:'JetBrains Mono',monospace;font-weight:600;font-size:.75rem"
    def s_page(v):
        return "color:#8b5cf6;font-family:'JetBrains Mono',monospace;font-size:.73rem"
    def s_mono(v):
        return "color:#334155;font-family:'JetBrains Mono',monospace;font-size:.71rem"

    ec  = ["EVENT TYPE"]  if "EVENT TYPE"  in df_show.columns else []
    pc  = ["PAGE URL"]    if "PAGE URL"    in df_show.columns else []
    mc  = [c for c in ["USER ID","DURATION MS","TIMESTAMP"] if c in df_show.columns]

    styled = (df_show.style
        .applymap(s_event, subset=ec)
        .applymap(s_page,  subset=pc)
        .applymap(s_mono,  subset=mc)
        .set_table_styles([{
            "selector":"th",
            "props":[("background","#080b16"),("color","#2d3a52"),
                     ("font-family","'JetBrains Mono',monospace"),("font-size","0.6rem"),
                     ("text-transform","uppercase"),("letter-spacing","0.12em"),
                     ("padding","11px 16px"),("border-bottom","1px solid rgba(255,255,255,0.06)"),
                     ("white-space","nowrap")]
        },{
            "selector":"td",
            "props":[("background","#0a0e1a"),("padding","9px 16px"),
                     ("border-color","rgba(255,255,255,0.04)")]
        },{
            "selector":"tr:nth-child(even) td",
            "props":[("background","rgba(0,212,188,0.015)")]
        },{
            "selector":"tr:hover td",
            "props":[("background","rgba(0,212,188,0.04)")]
        }])
    )
    st.dataframe(styled, use_container_width=True, hide_index=True, height=360)
else:
    st.markdown("""
    <div style="text-align:center;padding:48px;color:#2d3a52;
                font-family:'JetBrains Mono',monospace;font-size:.78rem">
      ⏳ Waiting for events to flow through the pipeline…
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  SPARK DEBUG
# ══════════════════════════════════════════════════════════════
with st.expander("🔥  Spark Processor — Debug Output"):
    d1,d2,d3,d4 = st.columns(4)
    d1.metric("Batches Done",    sp["batches_done"])
    d2.metric("Total Processed", sp["total_processed"])
    d3.metric("Avg Batch Size",  sp["avg_batch_size"])
    d4.metric("Avg Rate",        f"{sp['avg_event_rate']} ev/s")
    if summary: st.json(summary)

# ══════════════════════════════════════════════════════════════
#  FOOTER
# ══════════════════════════════════════════════════════════════
st.markdown(f"""
<div style="margin-top:40px;padding:20px 0;
            border-top:1px solid rgba(255,255,255,0.05);
            display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
  <span style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;color:#1a2235">
    NEXUS v2.0 &nbsp;·&nbsp; Kafka · Spark · Cassandra · Python · Streamlit
  </span>
  <span style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;color:#1a2235">
    {now.strftime('%d %b %Y · %H:%M:%S')}
  </span>
</div>
</div>
""", unsafe_allow_html=True)   # closes outer wrapper div

# ══════════════════════════════════════════════════════════════
#  AUTO REFRESH
# ══════════════════════════════════════════════════════════════
time.sleep(refresh)
st.rerun()