# app.py v1.3
#
# WHAT'S NEW (v1.3):
# - BUG FIX (CRITICAL): Fixed the competition grouping logic.
#   - The main loop now correctly parses AS competition/country
#     data from raw_data['league'] (e.g., 'league.country').
#   - This will fix the bug where 75 matches were not grouped.
# - RETAINED: All v1.2 logic.

import sys
import json
import os
import re
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Third-Party Libraries
import pandas as pd
import pytz
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from streamlit_js_eval import streamlit_js_eval

# Local Application Modules
import db 
from pwa import inject_pwa
from utils import (
    load_all_match_data,
    count_table,
    show_last_updated,
    parse_utc_to_gmt1
)
from widgets import (
    show_match_details,
    match_card_component,
    open_match_details,
)

# Run the autorefresh block every 300000 milliseconds (300000 seconds - 5mins)
st_autorefresh(interval=300000, key="data_refresher")

# === SAFE SQLALCHEMY IMPORT ===
try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError as e:
    st.error(f"psycopg2 import failed: {e}. Install via requirements.txt.")
    PSYCOPG2_AVAILABLE = False
    st.stop()

# === CONFIG ===
st.set_page_config(
    page_title="Atom Football",
    page_icon="static/icon-192.png",
    layout="wide",
    initial_sidebar_state="expanded",
)

# === PWA SETUP ===
inject_pwa()

# === ULTRA-RESPONSIVE CSS ===
# (Your vetted CSS is unchanged)
st.markdown(
    """
<style>
/* -------------------- DEFAULT STYLES (DESKTOP) -------------------- */
.match-wrapper { margin-bottom: 5px; }
.match-title-last7, .match-title-h2h {
    font-size: 0.6875rem; color: #666666; text-transform: uppercase;
    margin-bottom: 1px; border-top: 1px dashed #cccccc; padding-top: 2px;
    font-weight: 500; display: flex; justify-content: space-between;
}
.center-text { 
    text-align: center; font-weight: 900; font-size: 1.1em; line-height: 1.5; 
    margin: 0; 
}
.team-name-text { 
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; 
    line-height: 1.5; font-size: 1.0em; 
}
.result-indicator {
    display: flex; justify-content: center; align-items: center;
    width: 1.75rem; height: 1.75rem; border-radius: 4px; font-weight: 700;
    color: #f0f2f6; margin: 0 5px 0 0; font-size: 0.875rem; line-height: 1;
    text-transform: uppercase; flex-shrink: 0;
}
.match-divider-container { 
    display: flex; align-items: center; justify-content: center; 
    margin:0; height: 100%; 
}
.match-divider { 
    border: none; border-top: 1px dashed #999999; 
    border-bottom: 1px dashed #999999; margin: 1px; width: 100%; 
}
h3 { font-size: 1.25em !important; }

/* Custom Row Cards */
.day-row {
    background: #1a1a1a; padding: 12px; border-radius: 8px; margin-bottom: 8px;
    display: flex; justify-content: space-between; align-items: center;
    cursor: pointer; transition: 0.2s;
}
.day-row:hover { background: #2a2a2a; }
.comp-crest { width: 28px; height: 28px; object-fit: contain; margin-right: 10px; }
.comp-name { font-weight: 600; color: white; }
.comp-country { font-size: 0.75rem; color: #999; }
.match-count { 
    background: #333; color: white; padding: 4px 10px; border-radius: 20px; 
    font-size: 0.8rem; 
}
.match-card { 
    background: #222; padding: 10px; border-radius: 8px; margin-bottom: 8px;
    display: flex; justify-content: space-between; align-items: center; 
    cursor: pointer;
}
.match-card:hover { background: #333; }
.team-col { display: flex; align-items: center; gap: 8px; }
.team-crest { width: 24px; height: 24px; }
.team-name { font-size: 0.9rem; font-weight: 500; }
.pred-tags { font-size: 0.65rem; color: #4ade80; }
.time-date { text-align: right; font-size: 0.8rem; color: #aaa; }

/* Mobile */
@media (max-width: 640px) {
    .stSidebar [data-testid="stTitle"] { 
        font-size: 1.2rem !important; margin-bottom: 0.2rem !important; 
    }
    .stSidebar button { 
        padding: 0.3rem 0 !important; line-height: 1.2 !important; 
    }
    .stSidebar .stMultiSelect { padding: 0.5rem 0 !important; }
    .stApp [data-testid="stTitle"] { 
        margin-top: 0 !important; margin-bottom: 0.3rem !important; 
        font-size: 1.75rem !important; 
    }
    .stApp [data-testid="stCaptionContainer"] { 
        margin-bottom: 0.1rem !important; font-size: 0.65rem !important; 
    }
    .stApp [data-testid="stExpander"] { margin-bottom: 0.05rem !important; }
    .stApp [data-testid="stExpander"] button[data-testid="baseButton-secondary"] {
        padding: 0.2rem 0 !important; font-size: 0.8rem !important; 
        line-height: 1.2 !important;
    }
    h3 { font-size: 0.7em !important; }
    .match-wrapper { margin-bottom: 1px; }
    .match-title-last7, .match-title-h2h { 
        font-size: 0.5rem; padding-top: 1px; margin-bottom: 1px; 
    }
    .center-text { font-size: 0.7em; line-height: 1.1; }
    .team-name-text { font-size: 0.7em; line-height: 1.1; }
    .result-indicator { 
        width: 1.1rem; height: 1.1rem; font-size: 0.55rem; margin: 0 1px 0 0; 
    }
    .center-score-flex-container { 
        flex-wrap: nowrap !important; min-width: 90px; 
    }
}

/* === TAB STYLING (FIX) === */
button[data-testid="stTab"] {
    line-height: 1.3 !important;
    text-align: center !important;
    padding-top: 0.5rem !important;
    padding-bottom: 0.5rem !important;
}
/* This targets the <p> tag Streamlit uses for tab labels */
button[data-testid="stTab"] p {
    white-space: pre-wrap; /* This makes the \n newline character work */
    font-size: 0.9em;       /* Adjust font size as needed */
    font-weight: 600;       /* Make the text bold */
    line-height: 1.3;       /* Adjust line height for stacked text */
}

/* === SCROLLABLE TABS CONTAINER === */
div[data-baseweb="tab-list"] {
    flex-wrap: nowrap !important;
    overflow-x: auto !important;
    overflow-y: hidden !important;
    -webkit-overflow-scrolling: touch; /* Smooth scrolling on mobile */
}

/* Optional: Style the scrollbar for aesthetics */
div[data-baseweb="tab-list"]::-webkit-scrollbar {
    height: 4px; /* Make scrollbar smaller */
}
div[data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
    background: #444; /* Scrollbar color */
    border-radius: 2px;
}
div[data-baseweb="tab-list"]::-webkit-scrollbar-track {
    background: #222; /* Track color */
}

</style>
""",
    unsafe_allow_html=True,
)


def viewport():
    """Gets viewport width and stores it in session state."""
    viewport_width = streamlit_js_eval(
        js_expressions='window.innerWidth', wanted_result=True
    )

    if viewport_width is not None:
        st.session_state['display_width'] = viewport_width
        st.session_state['window_width'] = viewport_width
        return viewport_width
    else:
        st.session_state.setdefault('display_width', None)
        st.session_state.setdefault('window_width', None)


# === DB INIT (REMOVED THREADS) ===
if "initialized" not in st.session_state:
    con1 = st.container(#This is the only verifed way to centrailze the st.spinner horizontally and vertically. Every other method fails.
        height="stretch",
         width="stretch", 
         horizontal_alignment="center", 
         vertical_alignment="center",
         horizontal=True, 
    )
    with con1:
            with st.spinner("Connecting to database..."):
                viewport()  # Ensure we get viewport width early
                try:
                    if not db.test_connection():
                        st.error(
                            "Failed to connect to database after retries."
                        )
                        st.stop()
                except Exception as e:
                    st.error(f"DB Connection Failed: {e}")
                    st.stop()
                
                st.session_state.initialized = True
                time.sleep(1)
                st.rerun()

# === MAIN APP CONTAINER ===
with st.container():
    st.title("Football")

    # --- DYNAMIC TAB LOGIC ---
    display_width = st.session_state.get('display_width')
    window_width = st.session_state.get('window_width')

    if display_width is None:
        try:
            viewport()
        except Exception:
            pass
        display_width = st.session_state.get('display_width')
        window_width = st.session_state.get('window_width')

    if display_width is None:
        tabs_to_show = 3
    else:
        MIN_TABS = 3
        MAX_TABS = 15
        w = window_width if window_width is not None else display_width

        if w >= 1200:
            days_on_each_side = (MAX_TABS - 1) // 2
        elif 1130 <= w < 1200:
            days_on_each_side = 6
        elif 910 <= w < 1130:
            days_on_each_side = 5
        elif 785 <= w < 910:
            days_on_each_side = 4
        elif 565 <= w < 785:
            days_on_each_side = 4
        elif 345 <= w < 565:
            days_on_each_side = 2
        elif 200 <= w < 345:
            days_on_each_side = 2
        else:
            days_on_each_side = (MIN_TABS - 1) // 2

        tabs_to_show = 2 * days_on_each_side + 1

    # 2. Setup date/labels based on dynamic count
    gmt1_tz = pytz.timezone('Africa/Lagos')
    today_gmt1 = datetime.now(gmt1_tz).date()

    tab_labels = []
    tab_dates = []

    days_on_each_side = (tabs_to_show - 1) // 2

    for i in range(-days_on_each_side, days_on_each_side + 1):
        date = today_gmt1 + timedelta(days=i)
        tab_dates.append(date)
        day_name = date.strftime('%a').upper()
        date_str = date.strftime('%d %b').upper()
        if i == 0:
            label = f"TODAY\n{date_str}"
        else:
            label = f"{day_name}\n{date_str}"
        tab_labels.append(label)

    # === LOAD DATA BEFORE TABS ===
    if "matches_by_date" not in st.session_state:
        future_matches, past_matches = load_all_match_data()
        all_matches = future_matches + past_matches # all_matches is a list of dicts

        matches_by_date = {d: [] for d in tab_dates}
        for match in all_matches:
            try:
                # This logic is now safe thanks to utils.py v1.4
                date_str, _ = parse_utc_to_gmt1(match.get('utc_date'))
                mdate = datetime.strptime(date_str, "%d-%m-%Y").date()
                if mdate in matches_by_date:
                    matches_by_date[mdate].append(match)
            except Exception:
                continue

        st.session_state.matches_by_date = matches_by_date
        st.session_state.all_matches = all_matches
    else:
        # This logic is fine
        matches_by_date = st.session_state.matches_by_date
        all_matches = st.session_state.all_matches

        current_db_dates = set()
        for m in all_matches:
            try:
                date_str, _ = parse_utc_to_gmt1(m.get('utc_date'))
                current_db_dates.add(datetime.strptime(date_str, "%d-%m-%Y").date())
            except Exception:
                continue

        for date in tab_dates:
            if date not in matches_by_date and date in current_db_dates:
                matches_by_date[date] = [
                    m
                    for m in all_matches
                    if datetime.strptime(parse_utc_to_gmt1(m.get('utc_date'))[0], "%d-%m-%Y").date()
                    == date
                ]
            elif date not in matches_by_date:
                matches_by_date[date] = []

    # === VIEW HANDLER FOR DETAILS PAGE ===
    if "selected_match" in st.session_state and st.session_state.selected_match:
        if "view" in st.session_state and st.session_state.view:
            st.session_state['last_view'] = st.session_state.view
            del st.session_state["view"]

        show_match_details(st.session_state.selected_match)
        st.stop()
    
    # === MUTUALLY EXCLUSIVE RENDERING ===

    if not st.session_state.get("view"):
        # === MAIN TABS RENDER ===
        with st.container(border=True):
            today_label = "TODAY\n" + today_gmt1.strftime('%d %b').upper()

            tabs = st.tabs(tab_labels, default=today_label)
            
        # === RENDER EACH TAB ===
        for (tab, date) in zip(tabs, tab_dates):
            with tab:
                day_matches = matches_by_date.get(date)
                if not day_matches:
                    st.info(f"No matches on {date.strftime('%d %b, %Y').upper()}")
                    continue

                day_matches.sort(key=lambda m: parse_utc_to_gmt1(m.get('utc_date'))[1] or "99:99")

                all_matches_label = f"**All Matches** ({len(day_matches)})"
                if st.button(
                    all_matches_label, key=f"all_{date}", use_container_width=True
                ):
                    st.session_state.view = ("all", date)
                    st.rerun()

                comp_dict = {}
                
                # --- START v1.3 FIX: Final Bilingual Grouping ---
                for m in day_matches:
                    raw = m.get('raw_data', {})
                    if not raw:
                        continue 
                    
                    code = m.get('competition_code') # From DB Join
                    comp_name = "Unknown League"
                    comp_crest = None
                    comp_country = "Unknown Region"

                    # Default to FD structure
                    if 'competition' in raw and 'area' in raw:
                        raw_comp = raw.get('competition', {})
                        raw_area = raw.get('area', {})
                        comp_name = raw_comp.get("name", comp_name)
                        comp_crest = raw_comp.get("emblem")
                        comp_country = raw_area.get("name", comp_country)

                    # Override with AS structure
                    elif 'league' in raw:
                        raw_comp = raw.get('league', {})
                        comp_name = raw_comp.get("name", comp_name)
                        comp_crest = raw_comp.get("logo")
                        comp_country = raw_comp.get("country", comp_country) # AS stores country in 'league'
                    
                    if not code:
                        code = raw_comp.get('id', 'UNKNOWN') # Fallback
                        
                    if code not in comp_dict:
                        comp_dict[code] = {
                            "matches": [],
                            "name": comp_name,
                            "crest": comp_crest,
                            "country": comp_country,
                        }
                    comp_dict[code]["matches"].append(m)
                # --- END v1.3 FIX ---

                sorted_comps = sorted(
                    comp_dict.items(), key=lambda x: x[1]["country"]
                )

                # === Match Card Render Loop ===
                for code, data in sorted_comps:
                    expander_label = (
                        f"{data['country']} - {data['name']} ({len(data['matches'])})"
                    )
                    with st.expander(expander_label, expanded=False):
                        for match_data in data['matches']:
                            match_card_component(match_data)

    # === VIEW HANDLER for "All Matches" Page (Rendered only if view is set) ===
    if "view" in st.session_state and st.session_state.view:
        view_type, *args = st.session_state.view

        if st.button("Back to Day Overview"):
            st.session_state.view = None
            st.rerun()

        if view_type == "all":
            date = args[0]
            st.markdown("---")
            st.markdown("## All Matches")
            st.caption(f"{date.strftime('%A, %d %b, %Y').upper()}")

            matches = [
                m
                for m in all_matches
                if datetime.strptime(parse_utc_to_gmt1(m.get('utc_date'))[0], "%d-%m-%Y").date() == date
            ]
            matches.sort(key=lambda m: parse_utc_to_gmt1(m.get('utc_date'))[1] or "99:99")

            for m in matches:
                match_card_component(m)


# === SIDEBAR ===
# (This logic from v1.1 is correct)
upcoming_statuses = ['SCHEDULED', 'TIMED', 'TIME', 'POSTPONED']
past_statuses = ['FINISHED', 'IN_PLAY', 'PAUSED', 'CANCELED', 'SUSPENDED', 'AWARDED', 'ABANDONED']

st.sidebar.markdown(
    f"""
---
**Sync Status** - DB: {'Ready' if st.session_state.get('initialized') else 'Initializing'}
- Upcoming: {count_table('matches', upcoming_statuses)}
- Past: {count_table('matches', past_statuses)}
- Standings: {count_table('standings')}
""",
    unsafe_allow_html=True,
)

show_last_updated()