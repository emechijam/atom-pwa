# widgets.py v1.0
# Updated to work with psycopg2.pool and raw SQL (dict data)
# instead of SQLAlchemy ORM.

import streamlit as st
import re
from datetime import datetime
import pandas as pd
import sys 
import db  # <-- NEW: Import our new db module
from psycopg2.extras import RealDictRow
from typing import Dict, Any
# from fetch import enrich_single_match # (Removed)
from utils import format_date, get_structured_match_info, parse_utc_to_gmt1

# --- Standings Utility ---

def get_current_standing(league_code: str):
    """
    Fetches the most recent total standing table data (list of team rows) 
    for a given league code and the current season.
    (Updated for psycopg2)
    """
    conn = None
    try:
        conn = db.db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_data FROM standings
                WHERE competition_code = %s
                ORDER BY last_updated DESC
                LIMIT 1
                """,
                (league_code,)
            )
            standing_record = cur.fetchone()
            
            if standing_record and standing_record['table_data']:
                table_data = standing_record['table_data']
                # The rest of your parsing logic was already good for dicts
                if isinstance(table_data, list) and table_data:
                    for entry in table_data:
                        if entry.get('type') == 'TOTAL' and entry.get('table'):
                            return entry.get('table')
                    
                    if isinstance(table_data[0], dict) and table_data[0].get('table'):
                            return table_data[0].get('table')
                
                if isinstance(table_data, list) and isinstance(table_data[0], dict) and table_data[0].get('position'):
                        return table_data

    except Exception as e:
        print(f"Error loading standings for {league_code}: {e}", file=sys.stderr)
        return []
    finally:
        if conn:
            db.db_pool.putconn(conn)
    return []


# --- Tag Abbreviation Logic ---

TAG_MAP = {
    # (No changes to this dictionary)
    "Score no goal": "SNG",
    "Score At least a goal": "S1+",
    "Score At least 2 goals": "S2+",
    "Score At least 3 or more goals": "S3+",
    "Concede no goal": "CS", # Clean Sheet
    "Concede At least a goal": "C1+",
    "Concede At least 2 goals": "C2+",
    "Concede At least 3 or more goals": "C3+",
    "Win": "W",
    "Draw": "D",
    "Loss": "L",
    "Beats Strong Teams": "BST",
    "Loses to Weak Teams": "LWT",
    "H2H Dominance": "H2H",
    "Top vs Bottom": "T/B",
    "Close Rivals": "Rival",
    "Let's learn": ""
}

def render_tag_badges(tags_list: list):
    """
    Renders a series of st.badge() for the match card.
    (No database logic, no changes needed)
    """
    # (Your original, vetted UI code is unchanged)
    badge_style = (
        "display: inline-block; "
        "padding: 0.25em 0.4em; "
        "font-size: 75%; "
        "font-weight: 700; "
        "line-height: 1; "
        "text-align: center; "
        "white-space: nowrap; "
        "vertical-align: baseline; "
        "border-radius: 0.25rem; "
        "color: white; "
        "margin-right: 4px;"
    )
    colors = {
        "success": "#28a745", # Green
        "error": "#dc3545",   # Red
        "secondary": "#6c757d", # Grey
        "info": "#0d6efd",    # Blue
    }
    
    html = ""
    with st.container(horizontal=True):
        for tag in tags_list:
            abbr = TAG_MAP.get(tag, "N/A") 
            color_key = "info" 
            if abbr == "W":
                color_key = "success"
            elif abbr == "L":
                color_key = "error"
            elif abbr == "D":
                color_key = "secondary"
            elif abbr == "N/A":
                continue 

            bg_color = colors[color_key]
            html += f'<span style="{badge_style} background-color: {bg_color};">{abbr}</span>'
        
        st.markdown(html, unsafe_allow_html=True)


# --- Match Card Component ---

def match_card_component(match_data: Dict[str, Any]): # <-- Type hint changed to Dict
    """
    Renders a match card component for a single match.
    (Updated to use dict keys instead of object attributes)
    """
    match_card = st.container(border=True)
    
    # Extract prediction tags safely
    # 'prediction_data' is the column name from the LEFT JOIN
    prediction = match_data.get('prediction_data') or {"home_tags": ["N/A"], "away_tags": ["N/A"]}
    home_tags_list = prediction.get("home_tags", ["N/A"])
    away_tags_list = prediction.get("away_tags", ["N/A"])

    # --- Get Score and Status ---
    raw = match_data.get('raw_data', {}) # Use .get() for safety
    status = match_data.get('status')
    score = raw.get('score', {})
    full_time = score.get('fullTime', {})
    home_score = full_time.get('home')
    away_score = full_time.get('away')
    winner = score.get('winner')

    # --- Determine Score Badges ---
    # (No changes to this logic)
    home_score_badge = ""
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED'] and home_score is not None:
        home_score_badge = str(home_score)

    away_score_badge = ""
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED'] and away_score is not None:
        away_score_badge = str(away_score)

    # --- Determine Status Badge ---
    status_badge_label = ""
    status_badge_type = "secondary" 

    if status in ['IN_PLAY', 'PAUSED']:
        status_badge_label = "LIVE"
        status_badge_type = "error" 
    elif status == 'FINISHED':
        status_badge_label = "ENDED"
        status_badge_type = "info" 
    elif status in ['SUSPENDED', 'POSTPONED', 'CANCELLED']:
        status_badge_label = "PPD"
        status_badge_type = "warning"
    else: # SCHEDULED / TIMED
        # --- FIX: Derive time from utc_date ---
        _, time_gmt1 = parse_utc_to_gmt1(match_data.get('utc_date'))
        status_badge_label = time_gmt1[:5] # Show HH:MM
        status_badge_type = "secondary"
    
    with match_card:
        # --- Home Team Row ---
        # (Your vetted UI/UX logic is unchanged)
        winner_check = score.get('winner')
        home_border = True if (status == 'FINISHED' and winner_check == 'HOME_TEAM') else False

        if status == 'FINISHED':
            if winner_check == 'HOME_TEAM':
                home_badge_color_key = 'green'
            elif winner_check == 'DRAW':
                home_badge_color_key = 'gray'
            else:
                home_badge_color_key = 'red'
        else:
            home_badge_color_key = 'blue' 

        with st.container(horizontal=True, vertical_alignment="center"):
            home_crest = raw.get('homeTeam', {}).get('crest')
            if home_crest:
                st.image(home_crest, width=32)
            else:
                st.markdown("⚽️")
            
            home_name = raw.get('homeTeam', {}).get('shortName', 'Home')
            
            st.subheader(home_name)
            st.space("stretch") 
            
            if home_score_badge:
                badge_style = (
                    "display: inline-block; padding: 0.25em 0.4em; font-size: 75%; "
                    "font-weight: 700; line-height: 1; text-align: center; "
                    "white-space: nowrap; vertical-align: baseline; "
                    "border-radius: 0.25rem; color: white;"
                )
                colors = {
                    "green": "#28a745", "red": "#dc3545",
                    "gray": "#6c757d", "blue": "#0d6efd"
                }
                bg_color = colors.get(home_badge_color_key, colors["blue"])
                html = f'<span style="{badge_style} background-color: {bg_color};">{home_score_badge}</span>'
                st.markdown(html, unsafe_allow_html=True)
                
        with st.container(horizontal=True, vertical_alignment="center", horizontal_alignment="right", width="stretch", gap=None):
            render_tag_badges(home_tags_list)

        # --- Away Team Row ---
        # (Your vetted UI/UX logic is unchanged)
        if status == 'FINISHED':
            if winner_check == 'AWAY_TEAM':
                badge_color_key = 'green'
            elif winner_check == 'DRAW':
                badge_color_key = 'gray'
            else:
                badge_color_key = 'red'
        else:
            badge_color_key = 'blue' 

        away_border = True if (status == 'FINISHED' and winner_check == 'AWAY_TEAM') else False
        with st.container(horizontal=True, vertical_alignment="center"):
            away_crest = raw.get('awayTeam', {}).get('crest')
            if away_crest:
                st.image(away_crest, width=32)
            else:
                st.markdown("⚽️")
            
            away_name = raw.get('awayTeam', {}).get('shortName', 'Away')
            st.subheader(away_name)
            st.space("stretch")
            
            if away_score_badge:
                badge_style = (
                    "display: inline-block; padding: 0.25em 0.4em; font-size: 75%; "
                    "font-weight: 700; line-height: 1; text-align: center; "
                    "white-space: nowrap; vertical-align: baseline; "
                    "border-radius: 0.25rem; color: white;"
                )
                colors = {
                    "green": "#28a745", "red": "#dc3545",
                    "gray": "#6c757d", "blue": "#0d6efd"
                }
                bg_color = colors.get(badge_color_key, colors["blue"])
                html = f'<span style="{badge_style} background-color: {bg_color};">{away_score_badge}</span>'
                st.markdown(html, unsafe_allow_html=True)

        with st.container(horizontal=True, vertical_alignment="center", horizontal_alignment="right", width="stretch", gap=None):
            render_tag_badges(away_tags_list)

        # --- Status Row ---
        # (Your vetted UI/UX logic is unchanged)
        with st.container(horizontal=True, vertical_alignment="center"):
            badge_style = (
                "display: inline-block; "
                "padding: 0.25em 0.4em; "
                "font-size: 75%; "
                "font-weight: 700; "
                "line-height: 1; "
                "text-align: center; "
                "white-space: nowrap; "
                "vertical-align: baseline; "
                "border-radius: 0.25rem; "
                "color: white;"
            )
            colors = {
                "error": "#dc3545",   # Red (LIVE)
                "info": "#0d6efd",    # Blue (ENDED)
                "warning": "#ffc107", # Yellow (PPD)
                "secondary": "#6c757d" # Grey (Scheduled)
            }
            bg_color = colors.get(status_badge_type, colors["secondary"]) 
            
            html = f'<span style="{badge_style} background-color: {bg_color};">{status_badge_label}</span>'
            st.markdown(html, unsafe_allow_html=True)
        
        # --- Button ---
        # --- FIX: Use 'match_id' from dict ---
        st.button("Match Details", key=f"details_{match_data['match_id']}", 
                    on_click=open_match_details, args=(match_data,), use_container_width=True)


def open_match_details(match: Dict[str, Any]): # <-- Type hint changed to Dict
    """Callback to set the selected match in session state."""
    st.session_state.selected_match = match

# --- Callbacks for new buttons ---
# (No database logic, no changes needed)
def open_competition_page(league_code, league_name):
    """Callback to set the view to a specific competition."""
    st.session_state.view = ("competition", league_code, league_name)
    st.session_state.selected_match = None
    st.rerun()

def open_team_page(team_id, team_name):
    """Callback to set the view to a specific team."""
    st.session_state.view = ("team", team_id, team_name)
    st.session_state.selected_match = None
    st.rerun()
# --- END OF UPDATE ---


# --- Match Details Page (Updated) ---

def show_match_details(match: Dict[str, Any]): # <-- Type hint changed to Dict
    """
    Displays the full details page for a selected match.
    (Updated to use dict keys and parse utc_date)
    """

    if st.button("←"):
        st.session_state.selected_match = None
        if 'last_view' in st.session_state:
            st.session_state.view = st.session_state.last_view
            del st.session_state['last_view']
        else:
            st.session_state.view = None 
        st.rerun()

    raw = match.get('raw_data', {})
    home = raw.get('homeTeam', {})
    away = raw.get('awayTeam', {})
    home_name = home.get('shortName', 'Home Team')
    away_name = away.get('shortName', 'Away Team')
    home_crest = home.get('crest')
    away_crest = away.get('crest')
    
    # --- FIX: Derive date/time from utc_date ---
    date_gmt1, time_gmt1 = parse_utc_to_gmt1(match.get('utc_date'))
    date_time = f"{date_gmt1} {time_gmt1[:5]}"
    league_code = match.get('competition_code')
    league = raw.get('competition', {}).get('name', league_code)
    stage = raw.get('stage', 'N/A')

    # --- Get score and status logic ---
    status = match.get('status')
    score = raw.get('score', {})
    full_time = score.get('fullTime', {})
    home_score = full_time.get('home')
    away_score = full_time.get('away')

    # Scores to display
    home_score_display = "-"
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED'] and home_score is not None:
        home_score_display = str(home_score)

    away_score_display = "-"
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED'] and away_score is not None:
        away_score_display = str(away_score)

    # Status badge logic
    # (No changes to this logic)
    status_badge_label = ""
    status_badge_type = "secondary" 

    if status in ['IN_PLAY', 'PAUSED']:
        status_badge_label = "LIVE"
        status_badge_type = "error" 
    elif status == 'FINISHED':
        status_badge_label = "ENDED"
        status_badge_type = "info" 
    elif status in ['SUSPENDED', 'POSTPONED', 'CANCELLED']:
        status_badge_label = "PPD"
        status_badge_type = "warning"
    else: # SCHEDULED / TIMED
        status_badge_label = "SCHEDULED"
        status_badge_type = "secondary"


    # --- COMPETITION HEADER ---
    # (Your vetted UI/UX logic is unchanged)
    country_flag = raw.get('area', {}).get('flag')
    country_name = raw.get('area', {}).get('name', 'Unknown Country')
    
    with st.container(horizontal=True, vertical_alignment="center"):
        if country_flag:
            st.image(country_flag, width=60)
        else:
            st.markdown("🌐")
        st.button(f"{country_name} : {league} - {stage}", 
                    on_click=open_competition_page, 
                    args=(league_code, league)) # <-- Use league_code
            
        st.space("stretch") 
    st.markdown("---") 

    # --- HEADER WITH CRESTS & SCORE ---
    # (Your vetted UI/UX logic is unchanged)
    with st.container(horizontal=True, vertical_alignment="center", horizontal_alignment="center", width="stretch"):      
        st.space("stretch")
        st.write(date_time)
        st.space("small") 

    with st.container(horizontal=True, vertical_alignment="center", horizontal_alignment="center", width="stretch", gap=None): 
        with st.container(vertical_alignment="center", horizontal_alignment="center", width="stretch"):
            if home_crest:
                st.image(home_crest, width=64)
            st.button(home_name, on_click=open_team_page, args=(home.get('id'), home_name))
        
        badge_style = (
            "display: inline-block; padding: 0.25em 0.4em; font-size: 75%; "
            "font-weight: 700; line-height: 1; text-align: center; "
            "white-space: nowrap; vertical-align: baseline; "
            "border-radius: 0.25rem; color: white;"
            "background-color: #6c757d;"
        )
        html_home = f'<span style="{badge_style}">{home_score_display}</span>'
        html_away = f'<span style="{badge_style}">{away_score_display}</span>'
        
        st.markdown(html_home, unsafe_allow_html=True)
        st.write("") 
        st.markdown(html_away, unsafe_allow_html=True)
        
        with st.container(vertical_alignment="center", horizontal_alignment="center", width="stretch"): 
            if away_crest:
                st.image(away_crest, width=64)
            st.button(away_name, on_click=open_team_page, args=(away.get('id'), away_name))
            
    with st.container(horizontal=True, vertical_alignment="center", horizontal_alignment="center", width="stretch"):    
        badge_style = (
            "display: inline-block; "
            "padding: 0.25em 0.4em; "
            "font-size: 75%; "
            "font-weight: 700; "
            "line-height: 1; "
            "text-align: center; "
            "white-space: nowrap; "
            "vertical-align: baseline; "
            "border-radius: 0.25rem; "
            "color: white;"
        )
        colors = {
            "error": "#dc3545",   # Red (LIVE)
            "info": "#0d6efd",    # Blue (ENDED)
            "warning": "#ffc107", # Yellow (PPD)
            "secondary": "#6c757d" # Grey (Scheduled)
        }
        bg_color = colors.get(status_badge_type, colors["secondary"])
        st.space("stretch") 
        html = f'<span style="{badge_style} background-color: {bg_color};">{status_badge_label}</span>'
        st.markdown(html, unsafe_allow_html=True)
        
    st.space("stretch")
    st.markdown("---") 
    # --- END OF HEADER ---
    
    # --- LEAGUE STANDINGS TABLE ---
    st.markdown("#### League Standings")
    table_data = get_current_standing(league_code) # <-- Use league_code

    if table_data:
        standings_list = []
        for row in table_data:
            standings_list.append({
                "Pos": row.get('position'),
                "Team": row.get('team', {}).get('shortName', row.get('team', {}).get('name', 'N/A')),
                "P": row.get('playedGames'),
                "W": row.get('won'),
                "D": row.get('draw'),
                "L": row.get('lost'),
                "GF": row.get('goalsFor'),
                "GA": row.get('goalsAgainst'),
                "GD": row.get('goalDifference'),
                "Pts": row.get('points'),
            })

        df = pd.DataFrame(standings_list)
        indices_to_highlight = []
        for idx, row in df.iterrows():
            if row['Team'] == home_name or row['Team'] == away_name:
                indices_to_highlight.append(idx)
                
        def highlight_rows(s):
            is_match = s.name in indices_to_highlight
            return ['background-color: #333'] * len(s) if is_match else [''] * len(s)

        st.dataframe(
            df.style.apply(highlight_rows, axis=1), 
            use_container_width=True, 
            hide_index=True
        )
    else:
        st.info("No current league standings found for this competition in the database.")

    st.markdown("---")
    
    # --- Prediction Tags (Shows FULL TEXT) ---
    # --- FIX: Use 'prediction_data' ---
    prediction = match.get('prediction_data') or {"home_tags": ["N/A"], "away_tags": ["N/A"]}
    st.markdown("#### Expert Prediction Tags")
    col_p1, col_2 = st.columns(2)
    with col_p1:
        st.markdown(f"**{home_name} Analysis:**")
        for tag in prediction.get("home_tags", []):
            st.markdown(f"- {tag}")
    with col_2:
        st.markdown(f"**{away_name} Analysis:**")
        for tag in prediction.get("away_tags", []):
            st.markdown(f"- {tag}")

    st.markdown("---")
    
    # --- Last 7 Games Section ---
    st.markdown("#### Recent Form (Last 7 Games)")
    
    # --- FIX: Use .get() for safe access ---
    # These keys (e.g., 'home_last7') do not exist in the new DB schema
    # See "Critical Note" in my response. This will gracefully show the
    # st.info() message as intended.
    home_last7 = match.get('home_last7')
    away_last7 = match.get('away_last7')
    
    if home_last7 and away_last7:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**{home_name}**")
            display_last7_match_list(home_name, home_last7 or [])
            st.space("small")
        with col2:
            st.markdown(f"**{away_name}**")
            display_last7_match_list(away_name, away_last7 or [])
            st.space("small")
    else:
        st.info("Recent form data is not yet available. This data is not currently gathered by the sync script.")


    # --- H2H Section ---
    st.markdown("#### Head-to-Head Encounters") 
    
    # --- FIX: Use .get() for safe access ---
    h2h_data = match.get('h2h')
    h2h_count = match.get('h2h_count', 0)
    
    if h2h_data:
        st.info(f"Total H2H Matches: {h2h_count}")
        display_h2h_match_list(h2h_data[:5]) # Show most recent 5
    else:
        st.info("No Head-to-Head data available. This data is not currently gathered by the sync script.")


def display_last7_match_list(team_name: str, match_list: list):
    """
    Renders the 'Last 7' list using custom HTML.
    (No database logic, no changes needed)
    """
    if not match_list:
        st.info("No recent match data found.")
        return
    
    match_list = list(match_list)[::-1] 
    
    for match_data in match_list:
        info = get_structured_match_info(match_data, team_name)
        with st.container():
            st.markdown("<div class='match-wrapper'>", unsafe_allow_html=True)
            comp = match_data.get('competition', '')
            date = format_date(match_data.get('date_gmt1'))
            st.markdown(f"<div class='match-title-last7'><span>{date}</span><span>{comp}</span></div>", unsafe_allow_html=True)
            
            color = "#28a745" if info["is_win"] else "#dc3545" if info["is_loss"] else "#6c757d" if info["is_draw"] else "transparent"
            indicator = "W" if info["is_win"] else "L" if info["is_loss"] else "D" if info["is_draw"] else ""
            
            score_style_1 = "font-weight: 900;" if (info['target_is_team1'] and info['is_win']) or (info['target_is_team2'] and info['is_loss']) else ""
            score_style_2 = "font-weight: 900;" if (info['target_is_team2'] and info['is_win']) or (info['target_is_team1'] and info['is_loss']) else ""
            
            html = f"""
            <div style='display:flex;align-items:center;justify-content:space-between;'>
                <div class='result-indicator' style='background-color: {color};'>{indicator}</div>
                <div style='flex:2;display:flex;align-items:center;justify-content:center;position:relative;'>
                    <div class='team-name-text' style='flex:1;text-align:right;'>{info['team1_name']}</div>
                    <div class='center-score-flex-container' style='display:flex;align-items:center;justify-content:center;gap:6px;min-width:60px;'>
                        <span class='center-text' style='{score_style_1}'>{info['team1_score']}</span>
                        <span class='center-text'>:</span>
                        <span class='center-text' style='{score_style_2}'>{info['team2_score']}{info['extra_note']}</span>
                    </div>
                    <div class='team-name-text' style='flex:1;text-align:left;'>{info['team2_name']}</div>
                </div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

def display_h2h_match_list(match_list: list):
    """
    Renders the 'H2H' list using custom HTML.
    (No database logic, no changes needed)
    """
    if not match_list:
        st.info("No H2H data.")
        return
        
    match_list = list(match_list)[::-1]
    
    for match_data in match_list:
        info = get_structured_match_info(match_data, "") 
        with st.container():
            st.markdown("<div class='match-wrapper' style='margin:1px 0;'>", unsafe_allow_html=True)
            comp = match_data.get('competition', '')
            date = format_date(match_data.get('date_gmt1'))
            st.markdown(f"<div class='match-title-h2h'><span>{date} | {comp}</span></div>", unsafe_allow_html=True)
            
            html = f"""
            <div style='display:flex;align-items:center;justify-content:space-between;'>
                <div class='team-name-text' style='flex:1;text-align:left;'>{info['team1_name']}</div>
                <div class='center-score-flex-container' style='display:flex;align-items:center;justify-content:center;gap:6px;min-width:80px;'>
                    <span class='center-text'>{info['team1_score']}</span>
                    <span class='center-text'>:</span>
                    <span class='center-text'>{info['team2_score']}{info['extra_note']}</span>
                </div>
                <div class='team-name-text' style='flex:1;text-align:right;'>{info['team2_name']}</div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)