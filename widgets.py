# widgets.py v1.5
#
# WHAT'S NEW (v1.5):
# - UI/UX vetted for both mobile and desktop view 
# - UI BUG FIX: Modified 'show_match_details' to fix the
#   empty H2H and Recent Form sections.
# - DATA FLOW: The app now correctly reads 'h2h' and 'home_last7'
#   from *inside* the 'prediction_data' JSON object, which is
#   where 'predictor.py v1.4' now stores them.
# - RETAINED: All v1.3 logic for new standings tables.

import streamlit as st
import re
from datetime import datetime
import pandas as pd
import sys 
import db 
import logging # <-- Import logging
from psycopg2.extras import RealDictRow
from typing import Dict, Any, List
from utils import format_date, get_structured_match_info, parse_utc_to_gmt1

# --- Standings Utility (FIXED) ---

def get_current_standing(league_code: str) -> List[Dict[str, Any]]:
    """
    Fetches the most recent total standing table data (list of team rows)
    for a given league code and the current season.
    (v1.3: Rewritten to use the correct 'standings_lists' and
     'standing_rows' tables from populator.py v8.0)
    """
    # --- v1.3 FIX: Handle matches with no competition code ---
    if not league_code:
        logging.warning("get_current_standing called with no league_code.")
        return []
    # --- End Fix ---
    
    conn = None
    try:
        conn = db.db_pool.getconn()
        with conn.cursor() as cur:
            # This query joins all the necessary tables to find the
            # most recent "TOTAL" standing table for a given league code.
            sql = """
            WITH LatestStandingList AS (
                SELECT
                    sl.standings_list_id,
                    sl.stage,
                    sl.type,
                    sl.group_name
                FROM standings_lists sl
                JOIN competitions c ON sl.competition_id = c.competition_id
                WHERE
                    c.code = %s
                    AND sl.type = 'TOTAL'
                ORDER BY
                    sl.season_year DESC
                LIMIT 1
            )
            SELECT
                r.position,
                r.played_games,
                r.form,
                r.won,
                r.draw,
                r.lost,
                r.points,
                r.goals_for,
                r.goals_against,
                r.goal_difference,
                t.name as team_name,
                t.short_name as team_short_name,
                t.crest as team_crest
            FROM standing_rows r
            JOIN LatestStandingList lsl ON r.standings_list_id = lsl.standings_list_id
            JOIN teams t ON r.team_id = t.team_id
            ORDER BY
                r.position ASC;
            """
            cur.execute(sql, (league_code,))
            rows = cur.fetchall()
            
            if not rows:
                return []

            # Reconstruct the 'table' list that the UI expects
            table_data = []
            for row in rows:
                table_data.append({
                    "position": row['position'],
                    "team": {
                        "name": row['team_name'],
                        "shortName": row['team_short_name'],
                        "crest": row['team_crest']
                    },
                    "playedGames": row['played_games'],
                    "form": row['form'],
                    "won": row['won'],
                    "draw": row['draw'],
                    "lost": row['lost'],
                    "points": row['points'],
                    "goalsFor": row['goals_for'],
                    "goalsAgainst": row['goals_against'],
                    "goalDifference": row['goal_difference']
                })
            
            # The UI function expects the list, not the wrapper dict
            return table_data

    except Exception as e:
        # This will catch the "column does not exist" error
        # and any other SQL errors.
        print(f"Error loading standings for {league_code}: {e}", file=sys.stderr)
        return []
    finally:
        if conn:
            db.db_pool.putconn(conn)
    return []


# --- Tag Abbreviation Logic ---
# (Unchanged from v1.2)
TAG_MAP = {
    "Score no goal": "SNG", "Score At least a goal": "S1+",
    "Score At least 2 goals": "S2+", "Score At least 3 or more goals": "S3+",
    "Concede no goal": "CS", "Concede At least a goal": "C1+",
    "Concede At least 2 goals": "C2+", "Concede At least 3 or more goals": "C3+",
    "Win": "W", "Draw": "D", "Loss": "L", "Beats Strong Teams": "BST",
    "Loses to Weak Teams": "LWT", "H2H Dominance": "H2H",
    "Top vs Bottom": "T/B", "Close Rivals": "Rival", "Let's learn": ""
}

def render_tag_badges(tags_list: list):
    badge_style = (
        "display: inline-block; padding: 0.25em 0.4em; font-size: 75%; "
        "font-weight: 700; line-height: 1; text-align: center; "
        "white-space: nowrap; vertical-align: baseline; "
        "border-radius: 0.25rem; color: white; margin-right: 4px;"
    )
    colors = {
        "success": "#28a745", "error": "#dc3545",
        "secondary": "#6c757d", "info": "#0d6efd",
    }
    html = ""
    # Use st.container(horizontal=True) to manage layout in Streamlit
    tags_html = []
    for tag in tags_list:
        abbr = TAG_MAP.get(tag, "N/A") 
        color_key = "info" 
        if abbr == "W": color_key = "success"
        elif abbr == "L": color_key = "error"
        elif abbr == "D": color_key = "secondary"
        elif abbr == "N/A": continue 
        bg_color = colors[color_key]
        tags_html.append(f'<span style="{badge_style} background-color: {bg_color};">{abbr}</span>')
    
    st.markdown(" ".join(tags_html), unsafe_allow_html=True)


# --- Match Card Component ---
# (v1.4: Modified to use st.container for badge layout)
def match_card_component(match_data: Dict[str, Any]):
    match_card = st.container(border=True)
    
    # v1.4: prediction_data is now a dict, not a string
    prediction = match_data.get('prediction_data') or {}
    home_tags_list = prediction.get("home_tags", ["Let's learn"])
    away_tags_list = prediction.get("away_tags", ["Let's learn"])

    raw = match_data.get('raw_data', {})
    status = match_data.get('status')

    score = raw.get('score', {})
    full_time = score.get('fullTime', {})
    home_score = full_time.get('home')
    away_score = full_time.get('away')
    winner = score.get('winner')
    
    home_data_fd = raw.get('homeTeam', {})
    away_data_fd = raw.get('awayTeam', {})
    home_name = home_data_fd.get('shortName', 'Home')
    home_crest = home_data_fd.get('crest')
    away_name = away_data_fd.get('shortName', 'Away')
    away_crest = away_data_fd.get('crest')

    if 'teams' in raw:
        as_teams = raw.get('teams', {})
        home_data_as = as_teams.get('home', {})
        away_data_as = as_teams.get('away', {})
        
        home_name = home_data_as.get('name', 'Home')
        home_crest = home_data_as.get('logo')
        away_name = away_data_as.get('name', 'Away')
        away_crest = away_data_as.get('logo')

        as_score = raw.get('score', {})
        as_full_time = as_score.get('fulltime', {})
        home_score = as_full_time.get('home', raw.get('goals', {}).get('home'))
        away_score = as_full_time.get('away', raw.get('goals', {}).get('away'))
        
        winner_map = {True: 'HOME_TEAM', False: 'AWAY_TEAM', None: 'DRAW'}
        winner = winner_map.get(home_data_as.get('winner'))

    home_score_badge = ""
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED'] and home_score is not None:
        home_score_badge = str(home_score)

    away_score_badge = ""
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED'] and away_score is not None:
        away_score_badge = str(away_score)

    status_badge_label = ""
    status_badge_type = "secondary" 

    if status in ['IN_PLAY', 'PAUSED']:
        status_badge_label = "LIVE"
        status_badge_type = "error" 
    elif status == 'FINISHED':
        status_badge_label = "ENDED"
        status_badge_type = "info" 
    elif status in ['SUSPENDED', 'POSTPONED', 'CANCELLED', 'CANCELED']:
        status_badge_label = "PPD"
        status_badge_type = "warning"
    else: # SCHEDULED / TIMED / TIME
        _, time_gmt1 = parse_utc_to_gmt1(match_data.get('utc_date'))
        status_badge_label = time_gmt1[:5] # Show HH:MM
        status_badge_type = "secondary"
    
    with match_card:
        winner_check = winner
        if status == 'FINISHED':
            if winner_check == 'HOME_TEAM': home_badge_color_key = 'green'
            elif winner_check == 'DRAW': home_badge_color_key = 'gray'
            else: home_badge_color_key = 'red'
        else:
            home_badge_color_key = 'blue' 

        with st.container(horizontal=True, vertical_alignment="center"):
            with st.container(horizontal=True, vertical_alignment="center"):
                if home_crest: st.image(home_crest, width=40)
                else: st.markdown("⚽️")
                col1, col2 = st.columns([0.4,0.6], vertical_alignment="bottom")
                with col1:
                    st.subheader(home_name)
                with col2:
                    render_tag_badges(home_tags_list)
                st.space("stretch")
                if home_score_badge:
                    badge_style = ("display: inline-block; padding: 0.35em 0.5em; font-size: 100%; "
                        "font-weight: 700; line-height: 1; text-align: center; "
                        "white-space: nowrap; vertical-align: baseline; "
                        "border-radius: 0.25rem; color: white; float: right;")
                    colors = {"green": "#28a745", "red": "#dc3545", "gray": "#6c757d", "blue": "#0d6efd"}
                    bg_color = colors.get(home_badge_color_key, colors["blue"])
                    html = f'<span style="{badge_style} background-color: {bg_color};">{home_score_badge}</span>'
                    st.markdown(html, unsafe_allow_html=True)


        if status == 'FINISHED':
            if winner_check == 'AWAY_TEAM': badge_color_key = 'green'
            elif winner_check == 'DRAW': badge_color_key = 'gray'
            else: badge_color_key = 'red'
        else:
            badge_color_key = 'blue' 

        with st.container(horizontal=True, vertical_alignment="center"):
            with st.container(horizontal=True, vertical_alignment="center"):
                if away_crest: st.image(away_crest, width=40)
                else: st.markdown("⚽️")
                col1, col2 = st.columns([0.4,0.6], vertical_alignment="bottom")
                with col1:
                    st.subheader(away_name)
                with col2:
                    render_tag_badges(away_tags_list)
                st.space("stretch")
                if away_score_badge:
                    badge_style = ("display: inline-block; padding: 0.35em 0.5em; font-size: 100%; "
                        "font-weight: 700; line-height: 1; text-align: center; "
                        "white-space: nowrap; vertical-align: baseline; "
                        "border-radius: 0.25rem; color: white; float: right;")
                    colors = {"green": "#28a745", "red": "#dc3545", "gray": "#6c757d", "blue": "#0d6efd"}
                    bg_color = colors.get(badge_color_key, colors["blue"])
                    html = f'<span style="{badge_style} background-color: {bg_color};">{away_score_badge}</span>'
                    st.markdown(html, unsafe_allow_html=True)


        with st.container(horizontal=True, vertical_alignment="center"):
            badge_style = ("display: inline-block; padding: 0.25em 0.4em; font-size: 75%; "
                "font-weight: 700; line-height: 1; text-align: center; "
                "white-space: nowrap; vertical-align: baseline; "
                "border-radius: 0.25rem; color: white;")
            colors = {"error": "#dc3545", "info": "#0d6efd", "warning": "#ffc107", "secondary": "#6c757d"}
            bg_color = colors.get(status_badge_type, colors["secondary"]) 
            html = f'<span style="{badge_style} background-color: {bg_color};">{status_badge_label}</span>'
            st.markdown(html, unsafe_allow_html=True)
        
        st.button("Match Details", key=f"details_{match_data['match_id']}", 
                    on_click=open_match_details, args=(match_data,), use_container_width=True)


def open_match_details(match: Dict[str, Any]):
    st.session_state.selected_match = match
def open_competition_page(league_code, league_name):
    st.session_state.view = ("competition", league_code, league_name)
    st.session_state.selected_match = None
    st.rerun()
def open_team_page(team_id, team_name):
    st.session_state.view = ("team", team_id, team_name)
    st.session_state.selected_match = None
    st.rerun()


# --- Match Details Page (Updated) ---
# (v1.4: Modified to read H2H/Form from prediction JSON)
def show_match_details(match: Dict[str, Any]):
    if st.button("←"):
        st.session_state.selected_match = None
        if 'last_view' in st.session_state:
            st.session_state.view = st.session_state.last_view
            del st.session_state['last_view']
        else:
            st.session_state.view = None 
        st.rerun()

    raw = match.get('raw_data', {})
    
    # --- v1.4: Load prediction data from the start ---
    prediction = match.get('prediction_data') or {}
    
    # Defaults
    home_name = "Home Team"
    home_crest = None
    home_id = None
    away_name = "Away Team"
    away_crest = None
    away_id = None
    country_flag = None
    country_name = "Unknown Country"
    league = "Unknown League"
    stage = "N/A"
    home_score = None
    away_score = None

    # FD data structure
    if 'competition' in raw and 'area' in raw:
        home_data_fd = raw.get('homeTeam', {})
        away_data_fd = raw.get('awayTeam', {})
        home_name = home_data_fd.get('shortName', home_name)
        home_crest = home_data_fd.get('crest')
        home_id = home_data_fd.get('id')
        away_name = away_data_fd.get('shortName', away_name)
        away_crest = away_data_fd.get('crest')
        away_id = away_data_fd.get('id')
        
        area_data_fd = raw.get('area', {})
        country_flag = area_data_fd.get('flag')
        country_name = area_data_fd.get('name', country_name)
        
        comp_data_fd = raw.get('competition', {})
        league = comp_data_fd.get('name', league)
        stage = raw.get('stage', stage)
        
        score = raw.get('score', {})
        full_time = score.get('fullTime', {})
        home_score = full_time.get('home')
        away_score = full_time.get('away')

    # AS data structure (override if 'teams' key exists)
    elif 'teams' in raw and 'league' in raw:
        as_teams = raw.get('teams', {})
        home_data_as = as_teams.get('home', {})
        away_data_as = as_teams.get('away', {})
        
        home_name = home_data_as.get('name', home_name)
        home_crest = home_data_as.get('logo')
        home_id = home_data_as.get('id')
        away_name = away_data_as.get('name', away_name)
        away_crest = away_data_as.get('logo')
        away_id = away_data_as.get('id')

        as_league = raw.get('league', {})
        country_name = as_league.get('country', country_name)
        country_flag = as_league.get('flag')
        league = as_league.get('name', league)
        stage = as_league.get('round', stage)
        
        as_score = raw.get('score', {})
        as_full_time = as_score.get('fulltime', {})
        home_score = as_full_time.get('home', raw.get('goals', {}).get('home'))
        away_score = as_full_time.get('away', raw.get('goals', {}).get('away'))
    
    date_gmt1, time_gmt1 = parse_utc_to_gmt1(match.get('utc_date'))
    date_time = f"{date_gmt1} {time_gmt1[:5]}"
    league_code = match.get('competition_code')
    if not league:
        league = league_code

    status = match.get('status')
    home_score_display = "-"
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED'] and home_score is not None:
        home_score_display = str(home_score)

    away_score_display = "-"
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED'] and away_score is not None:
        away_score_display = str(away_score)

    status_badge_label = ""
    status_badge_type = "secondary" 
    if status in ['IN_PLAY', 'PAUSED']:
        status_badge_label = "LIVE"
        status_badge_type = "error" 
    elif status == 'FINISHED':
        status_badge_label = "ENDED"
        status_badge_type = "info" 
    elif status in ['SUSPENDED', 'POSTPONED', 'CANCELLED', 'CANCELED']:
        status_badge_label = "PPD"
        status_badge_type = "warning"
    else: # SCHEDULED / TIMED / TIME
        status_badge_label = "SCHEDULED"
        status_badge_type = "secondary"

    # --- COMPETITION HEADER ---
    with st.container(horizontal=True, vertical_alignment="center"): #, horizontal_alignment="center"):
        if country_flag:
            if isinstance(country_flag, str) and country_flag.endswith('.svg'):
                st.image(country_flag, width=40) #, width=60)
            elif isinstance(country_flag, str):
                st.image(country_flag, width=40) #, width=60)
            else:
                st.markdown("🌐")
        else:
            st.markdown("🌐")
            
        st.button(f"{country_name} : {league} - {stage}", 
                    on_click=open_competition_page, 
                    args=(league_code, league), width="stretch")
            
    #st.markdown("---") 

    # --- HEADER WITH CRESTS & SCORE ---
    with st.container(horizontal=True, vertical_alignment="center"): #, horizontal_alignment="center", width="stretch"):      
        date_time_style = "font-size: 0.8em; font-weight: 500; text-align: center; margin-top: 0px;"
        st.markdown(f"<div style='{date_time_style}'>{date_time}</div>", unsafe_allow_html=True)
            
        
    with st.container(horizontal=True, vertical_alignment="top"): #, horizontal_alignment="center", width="stretch", gap=None): 
        with st.container(vertical_alignment="center", horizontal_alignment="center"): #, width="stretch"):
            if home_crest:
                st.image(home_crest, width=64)
            st.button(home_name, on_click=open_team_page, args=(home_id, home_name), use_container_width=True)

        score_style = "font-size: 2.5em; font-weight: 700; text-align: center; margin-top: 24px;"
        st.markdown(f"<div style='{score_style}'>{home_score_display} - {away_score_display}</div>", unsafe_allow_html=True)      
        with st.container(vertical_alignment="center", horizontal_alignment="center"): #, width="stretch"): 
            if away_crest:
                st.image(away_crest, width=64)
            st.button(away_name, on_click=open_team_page, args=(away_id, away_name), use_container_width=True)
    with st.container(horizontal=True, vertical_alignment="center", horizontal_alignment="center"): #, width="stretch"):     
            badge_style = (
                "display: inline-block; padding: 0.25em 0.4em; font-size: 75%; "
                "font-weight: 700; line-height: 1; text-align: center; "
                "white-space: nowrap; vertical-align: baseline; "
                "border-radius: 0.25rem; color: white; float: center;"
            )
            colors = {"error": "#dc3545", "info": "#0d6efd", "warning": "#ffc107", "secondary": "#6c757d"}
            bg_color = colors.get(status_badge_type, colors["secondary"])
            html = f'<span style="{badge_style} background-color: {bg_color};">{status_badge_label}</span>'
            st.markdown(html, unsafe_allow_html=True)
    st.markdown("---") 
    
    # --- LEAGUE STANDINGS TABLE ---
    st.markdown("#### League Standings")
    
    # --- v1.3 FIX: This function is now safe to call ---
    table_data = get_current_standing(league_code) 

    if table_data:
        standings_list = []
        for row in table_data:
            # The data is already in the correct format
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
                
        def highlight_rows(row):
            if row.name in indices_to_highlight:
                return ['background-color: #333'] * len(row)
            return [''] * len(row)

        st.dataframe(
            df.style.apply(highlight_rows, axis=1), 
            use_container_width=True, 
            hide_index=True,
            column_config={"Pos": st.column_config.NumberColumn(width="small")}
        )
    else:
        st.info("No current league standings found for this competition in the database.")

    st.markdown("---")
    
    # --- Prediction Tags (Shows FULL TEXT) ---
    st.markdown("#### Expert Prediction Tags")
    col_p1, col_2 = st.columns(2)
    with col_p1:
        st.markdown(f"**{home_name} Analysis:**")
        # v1.4: Read from prediction dict
        for tag in prediction.get("home_tags", ["Let's learn"]):
            st.markdown(f"- {tag}")
    with col_2:
        st.markdown(f"**{away_name} Analysis:**")
        # v1.4: Read from prediction dict
        for tag in prediction.get("away_tags", ["Let's learn"]):
            st.markdown(f"- {tag}")

    st.markdown("---")
    
    # --- Last 7 Games Section ---
    st.markdown("#### Recent Form (Last 7 Games)")
    
    # --- v1.4 FIX: Read from *inside* the prediction JSON ---
    home_last7 = prediction.get('home_last7')
    away_last7 = prediction.get('away_last7')
    # --- End v1.4 Fix ---
    
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
        st.info("Recent form data has not been generated for this match yet.")


    # --- H2H Section ---
    st.markdown("#### Head-to-Head Encounters") 
    
    # --- v1.4 FIX: Read from *inside* the prediction JSON ---
    h2h_data = prediction.get('h2h')
    # --- End v1.4 Fix ---
    
    if h2h_data:
        st.info(f"Showing last {len(h2h_data)} H2H matches.")
        display_h2h_match_list(h2h_data) # Show most recent 5
    else:
        st.info("No Head-to-Head data available for these teams.")


def display_last7_match_list(team_name: str, match_list: list):
    if not match_list:
        st.info("No recent match data found.")
        return
    # v1.4: Data is already sorted by predictor
    # match_list = list(match_list)[::-1] 
    for match_data in match_list:
        # v1.4: Re-structure data for get_structured_match_info
        ui_match_data = {
            "result": (
                f"{match_data['home_team']} {match_data['home_goals']}-"
                f"{match_data['away_goals']} {match_data['away_team']}"
            ),
            "competition": match_data.get('competition_code', 'N/A'),
            "date_gmt1": match_data.get('date') # Already in YYYY-MM-DD
        }
        
        info = get_structured_match_info(ui_match_data, team_name)
        with st.container(border=True):
            comp = info.get('competition', ui_match_data.get('competition'))
            date = format_date(ui_match_data.get('date_gmt1').split(" ")[0].replace("-", "-"))

            st.caption(f"{date} | {comp}")
            
            color = "#28a745" if info["is_win"] else "#dc3545" if info["is_loss"] else "#6c757d" if info["is_draw"] else "transparent"
            indicator = "W" if info["is_win"] else "L" if info["is_loss"] else "D" if info["is_draw"] else ""
            
            score_style_1 = "font-weight: 900;" if (info['target_is_team1'] and info['is_win']) or (info['target_is_team2'] and info['is_loss']) else ""
            score_style_2 = "font-weight: 900;" if (info['target_is_team2'] and info['is_win']) or (info['target_is_team1'] and info['is_loss']) else ""
            
            html = f"""
            <div style='display:flex; align-items:center; justify-content:space-between; width:100%;'>
                <div style='background-color:{color}; color:white; border-radius:4px; padding: 2px 6px; font-weight:700; font-size:0.9em;'>{indicator}</div>
                <div style='flex:1; text-align:right; padding-right:10px; {score_style_1}'>{info['team1_name']}</div>
                <div style='font-weight:700;'>{info['team1_score']} - {info['team2_score']}</div>
                <div style='flex:1; text-align:left; padding-left:10px; {score_style_2}'>{info['team2_name']}</div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)


def display_h2h_match_list(match_list: list):
    if not match_list:
        st.info("No H2H data.")
        return
    # v1.4: Data is already sorted
    # match_list = list(match_list)[::-1]
    for match_data in match_list:
        # v1.4: Re-structure data
        ui_match_data = {
            "result": (
                f"{match_data['home_team']} {match_data['home_goals']}-"
                f"{match_data['away_goals']} {match_data['away_team']}"
            ),
            "competition": match_data.get('competition_code', 'N/A'),
            "date_gmt1": match_data.get('date') # Already in YTYY-MM-DD
        }

        info = get_structured_match_info(ui_match_data, "") 
        with st.container(border=True):
            comp = info.get('competition', ui_match_data.get('competition'))
            date = format_date(ui_match_data.get('date_gmt1').split(" ")[0].replace("-", "-"))
            
            st.caption(f"{date} | {comp}")
            
            html = f"""
            <div style='display:flex; align-items:center; justify-content:space-between; width:100%;'>
                <div style='flex:1; text-align:right; padding-right:10px;'>{info['team1_name']}</div>
                <div style='font-weight:700;'>{info['team1_score']} - {info['team2_score']}</div>
                <div style='flex:1; text-align:left; padding-left:10px;'>{info['team2_name']}</div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)