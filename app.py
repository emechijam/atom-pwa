# app.py v1.14
#
# WHAT'S NEW (v1.14 - CRITICAL DATE PARSING FIX):
# - CRITICAL FIX 1: Corrected date parsing format string from "%d-%m-%Y" to 
#   "%Y-%m-%d" in the main data loops (Tabs and Search Views) to align with 
#   the output of utils.parse_utc_to_gmt1. This resolves the persistent 
#   'time data does not match format' error and will allow fixtures to display.
# - RETAINED: All fixes and debug logging from v1.13.

# Standard Library Imports
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Third-Party Libraries Imports
import pandas as pd
import psutil
import psycopg2
import pytz
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from streamlit_js_eval import streamlit_js_eval

# Local Application Modules Imports
import db
from pwa import inject_pwa
import utils
from utils import (
    get_utc_date_range,
    parse_utc_to_gmt1,
)
from widgets import (
    fixture_card_component, 
    open_league_page,
    open_fixture_details, 
    open_team_page,
    show_fixture_details,
)

# CRITICAL FIX 1: Dummy reference to satisfy Pylance that the imported function is "used"
# The function is correctly used as a callback inside st.button in widgets.py.
_ = open_fixture_details 

# Run the autorefresh block every 300000 milliseconds (5mins)
# This keeps the FRONTEND data fresh. The sidebar toggle controls the BACKEND data.
st_autorefresh(interval=300000, key="data_refresher")

# === SAFE SQLALCHEMY IMPORT ===
try:
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
st.markdown(
    """
<style>
/* -------------------- DEFAULT STYLES (DESKTOP) -------------------- */
.fixture-wrapper { margin-bottom: 5px; }
.fixture-title-last7, .fixture-title-h2h {
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
.fixture-divider-container { 
    display: flex; align-items: center; justify-content: center; 
    margin:0; height: 100%; 
}
.fixture-divider { 
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
.league-crest { width: 28px; height: 28px; object-fit: contain; margin-right: 10px; }
.league-name { font-weight: 600; color: white; }
.country-name { font-size: 0.75rem; color: #999; }
.fixture-count { 
    background: #333; color: white; padding: 4px 10px; border-radius: 20px; 
    font-size: 0.8rem; 
}
.fixture-card { 
    background: #222; padding: 10px; border-radius: 8px; margin-bottom: 8px;
    display: flex; justify-content: space-between; align-items: center; 
    cursor: pointer;
}
.fixture-card:hover { background: #333; }
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
    /* v1.7: Adjust App Title size to match button */
    .stApp [data-testid="stTitle"] { 
        margin-top: 0.5rem !important; /* Align with search bar */
        margin-bottom: 0.3rem !important; 
        font-size: 1.75rem !important; 
        line-height: 1.3 !important;
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
    .fixture-wrapper { margin-bottom: 1px; }
    .fixture-title-last7, .fixture-title-h2h { 
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
button[data-testid="stTab"] p {
    white-space: pre-wrap;
    font-size: 0.9em; 
    font-weight: 600; 
    line-height: 1.3; 
}

/* === SCROLLABLE TABS CONTAINER === */
div[data-baseweb="tab-list"] {
    flex-wrap: nowrap !important;
    overflow-x: auto !important;
    overflow-y: hidden !important;
    -webkit-overflow-scrolling: touch; 
}
div[data-baseweb="tab-list"]::-webkit-scrollbar {
    height: 4px; 
}
div[data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
    background: #444; 
    border-radius: 2px;
}
div[data-baseweb="tab-list"]::-webkit-scrollbar-track {
    background: #222; 
}

/* v1.7: Style for the Title Button to look like st.title */
button[data-testid="baseButton-secondary"] p {
    font-size: 1.75rem !important;
    font-weight: 600;
}
/* Ensure the title button looks like a text */
button[data-testid="stButton"] > button.st-emotion-cache-19rxjzo {
    /* Reset button styles to look like text */
    border: none !important;
    background: transparent !important;
    color: white !important; /* Or your theme's title color */
    padding: 0 !important;
    margin: 0 !important; 
    text-align: left !important;
    
    /* Apply title font styling */
    font-size: 1.75rem !important; /* Match st.title */
    font-weight: 600 !important;
    line-height: 1.3 !important;
    display: inline !important; /* Act like text */
}
/* Remove hover effects */
button[data-testid="stButton"] > button.st-emotion-cache-19rxjzo:hover {
    background: transparent !important;
    color: #aaa !important; /* Slight dim on hover */
}
button[data-testid="stButton"] > button.st-emotion-cache-19rxjzo:active {
    background: transparent !important;
    color: #888 !important; /* Slight dim on active */
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
        st.session_state["display_width"] = viewport_width
        st.session_state["window_width"] = viewport_width
        return viewport_width
    else:
        st.session_state.setdefault("display_width", None)
        st.session_state.setdefault("window_width", None)


# === v1.5: PROCESS MANAGEMENT HELPERS ===
def is_process_running(pid):
    """Check if a process with the given PID is currently running."""
    if pid is None:
        return False
    try:
        p = psutil.Process(pid)
        return p.is_running()
    except psutil.NoSuchProcess:
        return False
    except Exception:
        return False


def kill_process_tree(pid):
    """Safely terminate a process and all its children."""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for child in children:
            child.terminate()
        parent.terminate()
        # Wait up to 3 seconds for termination
        psutil.wait_procs(children + [parent], timeout=3)
        st.sidebar.success("Live sync stopped.")
    except psutil.NoSuchProcess:
        st.sidebar.warning("Sync process was already stopped.")
    except Exception as e:
        st.sidebar.error(f"Error stopping process {pid}: {e}")
# === END v1.5 HELPERS ===


# === v1.7: MODAL AND HEADER FUNCTIONS ===
@st.dialog("Select Sport")
def show_sports_modal():
    """Shows the sport selection modal."""
    # Note: st.dialog holds the app execution until it's closed.
    sport_options = ["Football", "Basketball", "Tennis", "Baseball"]
    selected = st.radio(
        "Sport",
        sport_options,
        index=sport_options.index(
            st.session_state.get("selected_sport", "Football")
        ),
        horizontal=True,
    )

    st.session_state.selected_sport = selected

    if selected != "Football":
        st.info(f"{selected} support is coming soon!")

    if st.button("Close", use_container_width=True):
        st.rerun()


def render_header():
    """
    v1.7: Renders the main app header (Title Modal Button, Search, Toggle)
    """
    col_title, col_search = st.columns([1, 1], vertical_alignment="center")
    with col_title:
        # v1.7: Title is now a button that opens a modal
        sport_icon = "⚽️"  # Default
        if st.session_state.selected_sport == "Basketball":
            sport_icon = "🏀"
        elif st.session_state.selected_sport == "Tennis":
            sport_icon = "🎾"
        elif st.session_state.selected_sport == "Baseball":
            sport_icon = "⚾️"

        # Using markdown in the button label to control style
        if st.button(
            f"**{st.session_state.selected_sport}** {sport_icon}",
            key="sport_modal_button",
        ):
            show_sports_modal()

    with col_search:
        st.markdown("<br>", unsafe_allow_html=True)
        st.session_state.search_query = st.text_input(
            "Search",
            value=st.session_state.search_query,
            placeholder="Search teams, leagues, or players...",
            label_visibility="collapsed",
            width="stretch",
            icon=":material/search:",
        )

        st.session_state.filter_predictions_only = st.toggle(
            "Show Full Predictions Only",
            value=st.session_state.filter_predictions_only,
            help=(
                "If on, only shows fixtures that have generated H2H and Recent"
                " Form data (e.g., major leagues)."
            ),
        )
# === END v1.7 FUNCTIONS ===


# === v1.9: PAGINATION LOGIC (Renamed) ===
FIXTURE_PAGE_SIZE = 50


# Callback function to increment the limit and force a rerun
def load_more_fixtures():
    st.session_state.fixtures_limit += FIXTURE_PAGE_SIZE
    # We don't need to manually reset fetched data unless switching views


# === DB INIT ===
if "initialized" not in st.session_state:
    con1 = st.container(
        height="stretch",
        width="stretch",
        horizontal_alignment="center",
        vertical_alignment="center",
        horizontal=True,
    )
    with con1:
        with st.spinner("Connecting to database..."):
            viewport()  # Get viewport width
            try:
                # Test the connection by calling a real function from db.py
                db.get_match_counts() # Renamed later for consistency
            except Exception as e:
                # This catches the SQL COUNT() error if db.py v1.25 wasn't correctly loaded
                st.error(f"DB Connection Failed: {e}")
                st.stop()

            st.session_state.initialized = True
            time.sleep(1)
            st.rerun()

# --- Initialize Session State (Renamed) ---
if "search_query" not in st.session_state:
    st.session_state.search_query = ""
if "filter_predictions_only" not in st.session_state:
    st.session_state.filter_predictions_only = False
if "live_update_on" not in st.session_state:
    st.session_state.live_update_on = False
if "sync_process_pid" not in st.session_state:
    st.session_state.sync_process_pid = None
# v1.7: Sport Selection State
if "selected_sport" not in st.session_state:
    st.session_state.selected_sport = "Football"
# v1.9: Pagination State
if "fixtures_limit" not in st.session_state:
    st.session_state.fixtures_limit = FIXTURE_PAGE_SIZE


# Function to reset the pagination limit when the main view changes
def reset_pagination_limit(new_view=None):
    if st.session_state.fixtures_limit != FIXTURE_PAGE_SIZE:
        st.session_state.fixtures_limit = FIXTURE_PAGE_SIZE
    # Optional: clear cached matches if necessary, but relying on fetch is safer

# --- End Initialization ---


# === MAIN APP CONTAINER ===
with st.container():

    # --- v1.8: REVERTED TO DYNAMIC TAB LOGIC (from v1.6) ---
    display_width = st.session_state.get("display_width")
    window_width = st.session_state.get("window_width")

    if display_width is None:
        try:
            viewport()
        except Exception:
            pass
        display_width = st.session_state.get("display_width")
        window_width = st.session_state.get("window_width")

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
    gmt1_tz = pytz.timezone("Africa/Lagos")
    today_gmt1 = datetime.now(gmt1_tz).date()

    tab_labels = []
    tab_dates = []

    days_on_each_side = (tabs_to_show - 1) // 2

    for i in range(-days_on_each_side, days_on_each_side + 1):
        date = today_gmt1 + timedelta(days=i)
        tab_dates.append(date)
        day_name = date.strftime("%a").upper()
        date_str = date.strftime("%d %b").upper()
        if i == 0:
            label = f"TODAY\n{date_str}"
        else:
            label = f"{day_name}\n{date_str}"
        tab_labels.append(label)
    # === END v1.8 REVERT ===

    # === VIEW HANDLER FOR DETAILS PAGE (Highest precedence) ===
    if "selected_fixture" in st.session_state and st.session_state.selected_fixture:
        # Header is not rendered here; this is a full-page view
        show_fixture_details(st.session_state.selected_fixture)
        st.stop()

    # === v1.7: "All Fixtures" VIEW HANDLER (Renamed) ===
    # Moved up to be a primary view, and now includes the header.
    if (
        "view" in st.session_state
        and st.session_state.view
        and st.session_state.view[0] == "all"
    ):
        # Reset limit on first entry to this page if coming from another view
        if st.session_state.get("last_view_type") != "all":
            reset_pagination_limit()
            st.session_state.last_view_type = "all"

        # 1. RENDER HEADER
        render_header()

        # 2. RENDER VIEW CONTENT
        date = st.session_state.view[1]

        if st.button("← Back to Tabs", on_click=reset_pagination_limit):
            st.session_state.view = None
            st.session_state.last_view_type = None
            st.rerun()

        date_start, date_end = get_utc_date_range(date)

        # v1.8: Apply 7-day loading window logic for performance
        gmt1_now = datetime.now(gmt1_tz).date()
        min_date = gmt1_now - timedelta(days=7)
        max_date = gmt1_now + timedelta(days=7)

        fixtures = []
        if min_date <= date <= max_date:
            # v1.9: Get ALL fixtures for the day, then paginate in memory
            fixtures = db.get_filtered_matches(
                date_from=date_start,
                date_to=date_end,
                predictions_only=st.session_state.filter_predictions_only,
                limit=None,
                offset=0,  # Fetch all for in-memory pagination
            )

        st.markdown("---")
        st.markdown("## All Fixtures")
        st.caption(f"{date.strftime('%A, %d %b, %Y').upper()}")

        if not (min_date <= date <= max_date):
            st.info(
                f"Data for {date.strftime('%A, %d %b, %Y').upper()} is not"
                " pre-loaded. App only loads 7 days past/future by default."
            )
        elif fixtures:
            fixtures.sort(
                key=lambda m: parse_utc_to_gmt1(m.get("utc_date"))[1] or "99:99"
            )

            # v1.9: Pagination Logic
            limit = st.session_state.fixtures_limit
            fixtures_to_show = fixtures[:limit]

            for f in fixtures_to_show:
                fixture_card_component(f)

            if len(fixtures) > limit:
                st.button(
                    "Load More",
                    key="load_more_all_fixtures",
                    on_click=load_more_fixtures,
                    use_container_width=True,
                )
                st.caption(
                    f"Showing **{len(fixtures_to_show)}** of **{len(fixtures)}**"
                    " fixtures."
                )

        else:
            st.info(
                f"No fixtures found for {date.strftime('%A, %d %b, %Y').upper()}"
                " matching your filters."
            )

        st.stop()  # Stop execution after rendering this view

    # === SEARCH RESULTS VIEW (Renamed) ===
    if st.session_state.search_query.strip():
        # Reset limit on first entry to this page if coming from another view
        if st.session_state.get("last_view_type") != "search":
            reset_pagination_limit()
            st.session_state.last_view_type = "search"
            # Also reset cached search results if the query itself changed in the header
            if (
                st.session_state.get("last_search_key")
                != st.session_state.search_query.strip()
            ):
                if "search_fixtures" in st.session_state:
                    del st.session_state["search_fixtures"]

        # 1. RENDER HEADER
        render_header()

        # 2. RENDER VIEW CONTENT
        search_key = st.session_state.search_query.strip()
        st.header(f"Search Results for: '{search_key}'")

        search_results = db.search_teams_and_competitions(search_key)

        if search_results:
            st.subheader("Teams & Leagues")
            for item in search_results:
                search_card = st.container(border=True)
                with search_card:
                    col1, col2, col3 = st.columns([0.1, 0.7, 0.2])
                    with col1:
                        if item.get("emblem"):
                            st.image(item["emblem"], width=30)
                        elif item.get("type") == "team":
                            st.markdown("⚽️")
                        else:
                            st.markdown("🏆")
                    with col2:
                        st.markdown(f"**{item['name']}**")
                        st.caption(f"Type: {item['type'].capitalize()}")
                    with col3:
                        if item["type"] == "team":
                            st.button(
                                "View Team",
                                key=f"view_team_{item['id']}",
                                on_click=open_team_page,
                                args=(item["id"], item["name"]),
                                use_container_width=True,
                            )
                        elif item["type"] == "league":
                            st.button(
                                "View League",
                                key=f"view_league_{item['id']}",
                                on_click=open_league_page,
                                args=(item["id"], item["name"]),
                                use_container_width=True,
                            )
        else:
            st.info("No teams or leagues found matching your search term.")

        st.subheader("Fixtures (Upcoming & Recent)")

        # v1.8: Apply 7-day loading window logic for performance
        gmt1_now = datetime.now(gmt1_tz).date()
        date_from = (gmt1_now - timedelta(days=7)).strftime(
            "%Y-%m-%dT00:00:00Z"
        )
        date_to = (gmt1_now + timedelta(days=7)).strftime(
            "%Y-%m-%dT23:59:59Z"
        )

        if (
            "search_fixtures" not in st.session_state
            or st.session_state.get("last_search_key") != search_key
        ):
            all_filtered_fixtures = db.get_filtered_matches(
                date_from=date_from,
                date_to=date_to,
                search_query=search_key,
                predictions_only=st.session_state.filter_predictions_only,
                limit=None,
                offset=0,  # Fetch all for in-memory pagination
            )
            st.session_state.search_fixtures = all_filtered_fixtures
            st.session_state.last_search_key = search_key
        else:
            all_filtered_fixtures = st.session_state.search_fixtures

        if all_filtered_fixtures:

            # v1.9: Apply in-memory pagination to fixture list
            limit = st.session_state.fixtures_limit
            fixtures_to_display = all_filtered_fixtures[:limit]

            # Fixture grouping logic (kept for search results)
            fixtures_by_date = {}
            for fixture in fixtures_to_display:
                try:
                    date_gmt1, _ = parse_utc_to_gmt1(fixture["utc_date"])
                    # CRITICAL FIX 1: Change format string to YYYY-MM-DD
                    fdate = datetime.strptime(date_gmt1, "%Y-%m-%d").date()
                    fixtures_by_date.setdefault(fdate, []).append(fixture)
                except Exception as e:
                    # CRITICAL DEBUG: Print the source of the data dropping issue
                    print(f"DEBUG ERROR (Search View): Failed to parse date for fixture: {fixture.get('fixture_id')}. UTC Date: {fixture.get('utc_date')}. Error: {e}", file=sys.stderr)
                    continue

            # Sort the dates for display
            sorted_dates = sorted(fixtures_by_date.keys(), reverse=True)
            sorted_dates.sort(
                key=lambda date: date >= today_gmt1, reverse=True
            )

            for date in sorted_dates:
                fixtures_on_date = fixtures_by_date[date]
                fixtures_on_date.sort(
                    key=lambda f: parse_utc_to_gmt1(f.get("utc_date"))[1]
                    or "99:99"
                )

                expander_label = (
                    f"**{date.strftime('%A, %d %b, %Y').upper()}**"
                    f" ({len(fixtures_on_date)})"
                )
                with st.expander(expander_label, expanded=(date >= today_gmt1)):
                    for f in fixtures_on_date:
                        fixture_card_component(f)

            # v1.9: Load More button
            if len(all_filtered_fixtures) > limit:
                st.button(
                    "Load More",
                    key="load_more_search",
                    on_click=load_more_fixtures,
                    use_container_width=True,
                )
                st.caption(
                    f"Showing **{len(fixtures_to_display)}** of **{len(all_filtered_fixtures)}**"
                    " fixtures."
                )

        else:
            st.info(
                "No fixtures found matching your search and filter criteria"
                " within 7 days (past or future)."
            )

        st.stop()

    # === LEAGUE/TEAM VIEW HANDLER (Renamed) ===
    elif st.session_state.get("view"):
        # Reset limit on first entry to this page if coming from another view
        if st.session_state.get("last_view_type") != "league":
            reset_pagination_limit()
            st.session_state.last_view_type = "league"

        # 1. RENDER HEADER (Also shown on this page)
        render_header()

        # 2. RENDER VIEW CONTENT
        view_type, *args = st.session_state.view

        if st.button("← Back to Tabs", on_click=reset_pagination_limit):
            st.session_state.view = None
            st.session_state.last_view_type = None
            st.rerun()

        if view_type == "league":
            league_code, league_name = args
            st.header(f"{league_name}")

            # v1.8: Apply 7-day loading window logic for performance
            gmt1_now = datetime.now(gmt1_tz).date()
            date_from = (gmt1_now - timedelta(days=7)).strftime(
                "%Y-%m-%dT00:00:00Z"
            )
            date_to = (gmt1_now + timedelta(days=7)).strftime(
                "%Y-%m-%dT23:59:59Z"
            )

            # v1.9: Fetch all for league, then paginate in memory
            fixtures = db.get_filtered_matches(
                date_from=date_from,
                date_to=date_to,
                competition_code=league_code,
                predictions_only=st.session_state.filter_predictions_only,
                limit=None,
                offset=0,
            )

            if not fixtures:
                st.info(
                    "No fixtures found for this league that match your"
                    " filters (within 7 days past/future)."
                )
                st.stop()

            # v1.9: Apply in-memory pagination
            limit = st.session_state.fixtures_limit
            fixtures_to_display = fixtures[:limit]

            fixtures_by_date = {}
            for fixture in fixtures_to_display:
                try:
                    date_gmt1, _ = parse_utc_to_gmt1(fixture["utc_date"])
                    # CRITICAL FIX 1: Change format string to YYYY-MM-DD
                    fdate = datetime.strptime(date_gmt1, "%Y-%m-%d").date()
                    fixtures_by_date.setdefault(fdate, []).append(fixture)
                except Exception as e:
                    # CRITICAL DEBUG: Print the source of the data dropping issue
                    print(f"DEBUG ERROR (League View): Failed to parse date for fixture: {fixture.get('fixture_id')}. UTC Date: {fixture.get('utc_date')}. Error: {e}", file=sys.stderr)
                    continue

            sorted_dates = sorted(fixtures_by_date.keys())
            sorted_dates.sort(
                key=lambda date: date >= today_gmt1, reverse=True
            )

            for date in sorted_dates:
                fixtures_on_date = fixtures_by_date[date]
                fixtures_on_date.sort(
                    key=lambda f: parse_utc_to_gmt1(f.get("utc_date"))[1]
                    or "99:99"
                )

                expander_label = (
                    f"**{date.strftime('%A, %d %b, %Y').upper()}**"
                    f" ({len(fixtures_on_date)})"
                )
                with st.expander(
                    expander_label, expanded=(date == today_gmt1)
                ):
                    for f in fixtures_on_date:
                        fixture_card_component(f)

            # v1.9: Load More button
            if len(fixtures) > limit:
                st.button(
                    "Load More",
                    key="load_more_league",
                    on_click=load_more_fixtures,
                    use_container_width=True,
                )
                st.caption(
                    f"Showing **{len(fixtures_to_display)}** of **{len(fixtures)}**"
                    " fixtures."
                )

        elif view_type == "team":
            _, team_id, team_name = args
            st.header(f"{team_name}")
            st.info("Team-specific pages are under construction.")

        st.stop()

    # === MAIN TABS RENDER (Default view) ===

    # Reset limit when returning to tabs from Search/All Fixtures/League views
    if st.session_state.get("last_view_type") is not None:
        reset_pagination_limit()
        st.session_state.last_view_type = None

    # 1. RENDER HEADER
    render_header()

    # 2. RENDER TABS

    # Only render fixtures for the selected sport
    if st.session_state.selected_sport != "Football":
        st.info(f"Fixtures for {st.session_state.selected_sport} are not available yet.")
        st.stop()

    # v1.8: Apply 7-day loading window logic for performance
    gmt1_now = datetime.now(gmt1_tz).date()
    min_date = gmt1_now - timedelta(days=7)
    max_date = gmt1_now + timedelta(days=7)

    # Find the intersection of dates for the database query
    load_date_from = max(tab_dates[0], min_date).strftime(
        "%Y-%m-%dT00:00:00Z"
    )
    load_date_to = min(tab_dates[-1], max_date).strftime(
        "%Y-%m-%dT23:59:59Z"
    )

    # We fetch ALL fixtures for the visible date range (within 7-day constraint)
    all_tab_fixtures = []
    if tab_dates[0] <= max_date and tab_dates[-1] >= min_date:
        all_tab_fixtures = db.get_filtered_matches(
            date_from=load_date_from,
            date_to=load_date_to,
            predictions_only=st.session_state.filter_predictions_only,
            limit=None,
            offset=0,  # Fetch all fixtures for all relevant tabs
        )

    fixtures_by_date = {d: [] for d in tab_dates}
    for fixture in all_tab_fixtures:
        try:
            # FIX: Ensure fixture["utc_date"] is present before trying to access it
            if "utc_date" not in fixture or fixture["utc_date"] is None:
                raise ValueError("UTC date missing from fixture data.")
                
            date_str, _ = parse_utc_to_gmt1(fixture.get("utc_date"))
            # CRITICAL FIX 1: Change format string to YYYY-MM-DD to match utils.py output
            fdate = datetime.strptime(date_str, "%Y-%m-%d").date()
            if fdate in fixtures_by_date:
                fixtures_by_date[fdate].append(fixture)
        except Exception as e:
            # CRITICAL DEBUG: Print the source of the data dropping issue
            print(f"DEBUG ERROR (Main Tabs): Failed to parse date for fixture: {fixture.get('fixture_id')}. UTC Date: {fixture.get('utc_date')}. Error: {e}", file=sys.stderr)
            continue

    with st.container(border=True):
        today_label = "TODAY\n" + today_gmt1.strftime("%d %b").upper()

        # v1.8: Reverted to using default=today_label
        tabs = st.tabs(tab_labels, default=today_label)

    for (tab, date) in zip(tabs, tab_dates):
        with tab:
            # v1.8: Check if date is outside the 7-day load window
            if not (min_date <= date <= max_date):
                st.info(
                    f"Data for {date.strftime('%d %b, %Y').upper()} is not"
                    " pre-loaded. App only loads 7 days past/future by default."
                )
                continue

            day_fixtures = fixtures_by_date.get(date)
            if not day_fixtures:
                st.info(
                    f"No fixtures on {date.strftime('%d %b, %Y').upper()}"
                    " matching your filters."
                )
                continue

            day_fixtures.sort(
                key=lambda f: parse_utc_to_gmt1(f.get("utc_date"))[1] or "99:99"
            )

            all_fixtures_label = f"**All Fixtures** ({len(day_fixtures)})"
            if st.button(
                all_fixtures_label, key=f"all_{date}", use_container_width=True
            ):
                # When moving to 'all' view, set the view state
                st.session_state.view = ("all", date)
                st.rerun()

            league_dict = {}

            for f in day_fixtures:
                # v1.16: Use direct fields from db.py v1.16 query
                code = f.get("competition_code")
                
                # Skip if fixture has no league code (should be rare)
                if not code:
                    continue
                
                if code not in league_dict:
                    league_dict[code] = {
                        "fixtures": [],
                        "name": f.get("competition_name", "Unknown League"),
                        "crest": f.get("competition_crest"),
                        "country": f.get("competition_country", "Unknown Country"),
                        "has_prediction": False,
                    }

                # v1.8: Check if *any* fixture in this group has a prediction
                if (
                    f.get("prediction_data")
                    and f.get("prediction_data").get("h2h")
                    and f.get("prediction_data").get("h2h") != []
                ):
                    league_dict[code]["has_prediction"] = True

                league_dict[code]["fixtures"].append(f)

            sorted_leagues = sorted(
                league_dict.items(), key=lambda x: x[1]["country"]
            )

            for code, data in sorted_leagues:

                # v1.8: Logic for "Show Full Predictions Only"
                predictions_only = st.session_state.filter_predictions_only

                # If toggle is on, check if this group has predictions
                if predictions_only and not data["has_prediction"]:
                    continue  # Skip this league entirely

                # Count *only* fixtures with predictions if toggle is on
                if predictions_only:
                    fixtures_to_show = [
                        f
                        for f in data["fixtures"]
                        if f.get("prediction_data")
                        and f.get("prediction_data").get("h2h")
                        and f.get("prediction_data").get("h2h") != []
                    ]
                    fixture_count = len(fixtures_to_show)
                else:
                    fixtures_to_show = data["fixtures"]
                    fixture_count = len(fixtures_to_show)

                if fixture_count == 0:
                    continue

                expander_label = (
                    f"{data['country']} - {data['name']} ({fixture_count})"
                )

                # v1.8: Expander is open if the prediction toggle is ON
                with st.expander(
                    expander_label,
                    expanded=st.session_state.filter_predictions_only,
                ):
                    for fixture_data in fixtures_to_show:
                        fixture_card_component(fixture_data)


# === SIDEBAR ===

# --- v1.5: Live Update Process Management ---
st.sidebar.markdown("---")
st.sidebar.toggle("Live Updates", key="live_update_on")
st.sidebar.caption("Keeps DB in sync. App must stay open.")

current_pid = st.session_state.sync_process_pid
is_running = is_process_running(current_pid)
want_running = st.session_state.live_update_on

status_placeholder = st.sidebar.empty()

if want_running and not is_running:
    status_placeholder.info("Starting live sync process...")
    try:
        process = subprocess.Popen([sys.executable, "sync.py"])
        st.session_state.sync_process_pid = process.pid
        time.sleep(1)
        st.rerun()
    except Exception as e:
        status_placeholder.error(f"Failed to start sync.py: {e}")
        st.session_state.live_update_on = False

elif not want_running and is_running:
    status_placeholder.info(f"Stopping live sync process (PID: {current_pid})...")
    kill_process_tree(current_pid)
    st.session_state.sync_process_pid = None
    time.sleep(1)
    st.rerun()

if is_running:
    status_placeholder.success(f"Live sync is ON (PID: {current_pid})")
else:
    status_placeholder.warning("Live sync is OFF.")
    # FIX 1: Call db.get_last_updated_time() and display it
    last_update_time = db.get_last_updated_time()
    if last_update_time:
        # We can re-use the parse_utc_to_gmt1 function we imported from utils
        date_str, time_str = parse_utc_to_gmt1(last_update_time)
        st.sidebar.caption(f"DB last updated:\n{date_str} {time_str[:5]} (GMT+1)")
    else:
        st.sidebar.caption("DB last updated: Unknown")
# --- End v1.6 Process Management ---

# v1.9: VERSION DISPLAY
st.sidebar.info("Atom v3")

# --- v1.7: Sidebar Stats (PERFORMANCE FIX) ---
status_counts = db.get_match_counts() # Renamed later for consistency
upcoming_count = status_counts.get("UPCOMING", 0)
past_count = status_counts.get("PAST", 0) + status_counts.get("OTHER", 0)

st.sidebar.markdown(
    f"""
---
**DB Status** - Connection: {'Ready' if st.session_state.get('initialized') else 'Initializing'}
- Upcoming: {upcoming_count}
- Past: {past_count}
- Standings: {db.count_standings_lists()}
""",
    unsafe_allow_html=True,
)