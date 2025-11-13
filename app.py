# app.py v1.4
#
# WHAT'S NEW (v1.4):
# - SEARCH BAR: Added a 'st.text_input' for searching, placed
#   next to the main title using st.columns.
# - PREDICTION FILTER: Added a 'st.toggle' to filter for
#   matches that have "full" predictions (H2H, Form, etc.).
# - VIEW SWITCH: If a search query is active, the app switches
#   to a "search results" view that lists clickable entities (teams, competitions)
#   and filtered matches across all dates.
# - DATA FLOW REFACTOR (CRITICAL): Removed static loading of all matches
#   into session state (st.session_state.matches_by_date). Tab and View
#   renders now make targeted calls to db.get_filtered_matches, passing
#   the search/filter parameters.

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
    # load_all_match_data, # Removed static load
    count_table,
    show_last_updated,
    parse_utc_to_gmt1,
    get_utc_date_range # Utility assumed to be in utils.py
)
from widgets import (
    show_match_details,
    match_card_component,
    open_match_details,
    open_competition_page, # Assumed to be available from widgets.py v1.5
    open_team_page,        # Assumed to be available from widgets.py v1.5
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

# --- v1.4: Initialize Session State for Search & Filter ---
if 'search_query' not in st.session_state:
    st.session_state.search_query = ""
if 'filter_predictions_only' not in st.session_state:
    st.session_state.filter_predictions_only = False
# --- End v1.4 Search & Filter Initialization ---

# === MAIN APP CONTAINER ===
with st.container():
    
    # --- v1.4: Header with Title and Search Bar ---
    col_title, col_search = st.columns([1, 1])
    with col_title:
        st.title("Football")
    with col_search:
        st.markdown("<br>", unsafe_allow_html=True)
        st.session_state.search_query = st.text_input(
            "Search",
            value=st.session_state.search_query,
            placeholder="Search teams, competitions, or players...",
            label_visibility="collapsed", 
            width="stretch",
            icon=":material/search:"
        )

        # --- v1.4: Prediction Filter ---
        st.session_state.filter_predictions_only = st.toggle(
            "Show Full Predictions Only",
            value=st.session_state.filter_predictions_only,
            help="If on, only shows matches that have generated H2H and Recent Form data (e.g., major leagues)."
        )
   
    # --- End v1.4 UI Changes ---

    # --- DYNAMIC TAB LOGIC (Viewport calculation remains the same) ---
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

    # Clearing old session state match data as we now do targeted DB calls
    if "matches_by_date" in st.session_state:
        del st.session_state["matches_by_date"]
    if "all_matches" in st.session_state:
        del st.session_state["all_matches"]

    # === VIEW HANDLER FOR DETAILS PAGE (Highest precedence) ===
    if "selected_match" in st.session_state and st.session_state.selected_match:
        if "view" in st.session_state and st.session_state.view:
            st.session_state['last_view'] = st.session_state.view
            del st.session_state["view"]

        show_match_details(st.session_state.selected_match)
        st.stop()
    
    # === v1.4: Search Results View (Takes precedence over tabs) ===
    if st.session_state.search_query.strip():
        st.header(f"Search Results for: '{st.session_state.search_query.strip()}'")
        
        # 1. Search for clickable entities (Teams, Competitions)
        search_results = db.search_teams_and_competitions(st.session_state.search_query.strip())
        
        if search_results:
            st.subheader("Teams & Competitions")
            for item in search_results:
                search_card = st.container(border=True)
                with search_card:
                    col1, col2, col3 = st.columns([0.1, 0.7, 0.2])
                    with col1:
                        if item.get('emblem'):
                            st.image(item['emblem'], width=30)
                        elif item.get('type') == 'team':
                            st.markdown("⚽️")
                        else:
                            st.markdown("🏆")

                    with col2:
                        st.markdown(f"**{item['name']}**")
                        st.caption(f"Type: {item['type'].capitalize()}")

                    with col3:
                        if item['type'] == 'team':
                            st.button("View Team", key=f"view_team_{item['id']}", 
                                      on_click=open_team_page, args=(item['id'], item['name']),
                                      use_container_width=True)
                        elif item['type'] == 'competition':
                            # Assumes the DB returns 'code' for competitions
                            st.button("View Comp", key=f"view_comp_{item['id']}", 
                                      on_click=open_competition_page, args=(item['code'], item['name']),
                                      use_container_width=True)
                        # NOTE: Player button here when player view is ready
        else:
            st.info("No teams or competitions found matching your search term.")

        # 2. Match Search Results (The filtered matches for all dates)
        st.subheader("Matches (Upcoming & Recent)")
        
        # Fetches matches over a wide date range for context (past 30 days to future 30 days)
        # Using a fixed date range since exact data is not known, but fetching all matches is safer
        all_filtered_matches = db.get_filtered_matches(
            search_query=st.session_state.search_query,
            predictions_only=st.session_state.filter_predictions_only
        )

        if all_filtered_matches:
            # Group by date for cleaner display
            matches_by_date = {}
            for match in all_filtered_matches:
                date_gmt1, _ = parse_utc_to_gmt1(match['utc_date'])
                mdate = datetime.strptime(date_gmt1, "%d-%m-%Y").date()
                matches_by_date.setdefault(mdate, []).append(match)
            
            # Sort by date, prioritizing future matches
            sorted_dates = sorted(matches_by_date.keys(), reverse=True)
            sorted_dates.sort(key=lambda date: date >= today_gmt1, reverse=True)

            for date in sorted_dates:
                matches_on_date = matches_by_date[date]
                matches_on_date.sort(key=lambda m: parse_utc_to_gmt1(m.get('utc_date'))[1] or "99:99")
                
                expander_label = f"**{date.strftime('%A, %d %b, %Y').upper()}** ({len(matches_on_date)})"
                with st.expander(expander_label, expanded=(date >= today_gmt1)):
                    for m in matches_on_date:
                        match_card_component(m)
        else:
            st.info("No matches found matching your search and filter criteria across all dates.")
        
        st.stop()
    
    # === Competition/View Handler ===
    elif st.session_state.get("view"):
        view_type, *args = st.session_state.view
        
        if st.button("←"):
            st.session_state.view = None
            st.rerun()

        if view_type == "competition":
            league_code, league_name = args
            st.header(f"{league_name}")
            
            # Fetch and render matches for this competition (no search query needed here)
            matches = db.get_filtered_matches(
                competition_code=league_code,
                predictions_only=st.session_state.filter_predictions_only
            )
            
            if not matches:
                st.info("No matches found for this competition that match your filters.")
                st.stop()
            
            # Group by date and render
            matches_by_date = {}
            for match in matches:
                date_gmt1, _ = parse_utc_to_gmt1(match['utc_date'])
                mdate = datetime.strptime(date_gmt1, "%d-%m-%Y").date()
                matches_by_date.setdefault(mdate, []).append(match)
            
            sorted_dates = sorted(matches_by_date.keys())
            sorted_dates.sort(key=lambda date: date >= today_gmt1, reverse=True)

            for date in sorted_dates:
                matches_on_date = matches_by_date[date]
                matches_on_date.sort(key=lambda m: parse_utc_to_gmt1(m.get('utc_date'))[1] or "99:99")
                
                expander_label = f"**{date.strftime('%A, %d %b, %Y').upper()}** ({len(matches_on_date)})"
                with st.expander(expander_label, expanded=(date == today_gmt1)):
                    for m in matches_on_date:
                        match_card_component(m)
        
        elif view_type == "team":
             _, team_id, team_name = args
             st.header(f"{team_name}")
             st.info("Team-specific pages are under construction.")

        st.stop()
    
    # === MAIN TABS RENDER (Default view, no search query active) ===
    date_from = tab_dates[0].strftime('%Y-%m-%dT00:00:00Z')
    date_to = tab_dates[-1].strftime('%Y-%m-%dT23:59:59Z')
    
    all_tab_matches = db.get_filtered_matches(
        date_from=date_from,
        date_to=date_to,
        predictions_only=st.session_state.filter_predictions_only
    )
    
    matches_by_date = {d: [] for d in tab_dates}
    for match in all_tab_matches:
        try:
            date_str, _ = parse_utc_to_gmt1(match.get('utc_date'))
            mdate = datetime.strptime(date_str, "%d-%m-%Y").date()
            if mdate in matches_by_date:
                matches_by_date[mdate].append(match)
        except Exception:
            continue

    with st.container(border=True):
        today_label = "TODAY\n" + today_gmt1.strftime('%d %b').upper()

        tabs = st.tabs(tab_labels, default=today_label)
        
    for (tab, date) in zip(tabs, tab_dates):
        with tab:
            day_matches = matches_by_date.get(date)
            if not day_matches:
                st.info(f"No matches on {date.strftime('%d %b, %Y').upper()} matching your filters.")
                continue

            day_matches.sort(key=lambda m: parse_utc_to_gmt1(m.get('utc_date'))[1] or "99:99")

            # All Matches button for this day (now creates the 'all' view)
            all_matches_label = f"**All Matches** ({len(day_matches)})"
            if st.button(
                all_matches_label, key=f"all_{date}", use_container_width=True
            ):
                st.session_state.view = ("all", date)
                st.rerun()

            comp_dict = {}
            
            # --- START Grouping Logic ---
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
            # --- END Grouping Logic ---

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

    
    # === VIEW HANDLER for "All Matches" Page (Rendered only if view is set to 'all') ===
    if "view" in st.session_state and st.session_state.view and st.session_state.view[0] == "all":
        date = st.session_state.view[1]
        
        date_start, date_end = get_utc_date_range(date)
        matches = db.get_filtered_matches(
            date_from=date_start, 
            date_to=date_end, 
            predictions_only=st.session_state.filter_predictions_only
        )
        
        st.markdown("---")
        st.markdown("## All Matches")
        st.caption(f"{date.strftime('%A, %d %b, %Y').upper()}")

        if matches:
            matches.sort(key=lambda m: parse_utc_to_gmt1(m.get('utc_date'))[1] or "99:99")
            for m in matches:
                match_card_component(m)
        else:
             st.info(f"No matches found for {date.strftime('%A, %d %b, %Y').upper()} matching your filters.")


# === SIDEBAR ===
# To get correct counts, we must fetch all matches without filtering.
all_matches_for_stats = db.get_all_matches()
upcoming_statuses = ['SCHEDULED', 'TIMED', 'TIME', 'POSTPONED']
past_statuses = ['FINISHED', 'IN_PLAY', 'PAUSED', 'CANCELED', 'SUSPENDED', 'AWARDED', 'ABANDONED']

upcoming_count = len([m for m in all_matches_for_stats if m['status'] in upcoming_statuses])
past_count = len([m for m in all_matches_for_stats if m['status'] in past_statuses])

st.sidebar.markdown(
    f"""
---
**Sync Status** - DB: {'Ready' if st.session_state.get('initialized') else 'Initializing'}
- Upcoming: {upcoming_count}
- Past: {past_count}
- Standings: {count_table('standings_lists')}
""",
    unsafe_allow_html=True,
)

show_last_updated()