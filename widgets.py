# widgets.py v1.9
#
# WHAT'S NEW (v1.9 - TERMINOLOGY AND SCHEMA REFACTOR):
# - Renamed all instances of: match -> fixture, competition -> league.
# - CRITICAL FIX 1: Rewrote `get_current_standing` SQL to query the new,
#   single `standings` table, resolving the major bug. It also correctly
#   selects `t.logo_url`.
# - Renamed helper functions to reflect `fixture` and `league` terminology.

import streamlit as st
import re
from datetime import datetime
import pandas as pd
import sys 
import db 
import logging
from psycopg2.extras import RealDictRow, RealDictCursor 
from typing import Dict, Any, List
from utils import format_date, get_structured_match_info, parse_utc_to_gmt1
from db import get_h2h_data, get_last_7_home_data, get_last_7_away_data, get_tags 

# --- Standings Utility (FIXED FOR NEW SCHEMA) ---

@st.cache_data(ttl=300)
def get_current_standing(league_id: int) -> List[Dict[str, Any]]:
    """
    v1.9: Fetches and reconstructs the standing table from the new 'standings'
    schema.
    """
    if not league_id:
        logging.warning("get_current_standing called with no league_id.")
        return []
    
    conn = None
    try:
        conn = db.db_pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # v1.9: New query for the new flat 'standings' table
            sql = """
            WITH LatestSeason AS (
                -- First, find the latest season year for this league
                SELECT
                    s.season_year
                FROM standings s
                WHERE s.league_id = %s
                ORDER BY s.season_year DESC
                LIMIT 1
            )
            -- Now, get all standings for that league and season
            SELECT
                s.rank as position,
                s.played as played_games,
                s.form,
                s.win as won,
                s.draw,
                s.lose as lost,
                s.points,
                s.goals_for,
                s.goals_against,
                s.goals_diff as goal_difference,
                t.name as team_name,
                t.code as team_short_name,
                t.logo_url as team_crest -- FIX: Use 'logo_url'
            FROM standings s
            JOIN teams t ON s.team_id = t.team_id
            JOIN LatestSeason ls ON s.season_year = ls.season_year
            WHERE
                s.league_id = %s
            ORDER BY
                s.rank ASC;
            """
            cur.execute(sql, (league_id, league_id))
            rows = cur.fetchall()
            
            if not rows:
                return []

            # Reconstruct the 'table' list
            standings_data = []
            for row in rows:
                standings_data.append({
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
            
            return standings_data

    except Exception as e:
        print(f"Error loading standings for {league_id}: {e}", file=sys.stderr)
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


# --- Fixture Card Component ---
# (V1.9: Removed all raw_data logic, renamed)
def fixture_card_component(fixture_data: Dict[str, Any]):
    fixture_card = st.container(border=True)
    
    prediction = fixture_data.get('prediction_data') or {}
    home_tags_list = prediction.get("home_tags", ["Let's learn"])
    away_tags_list = prediction.get("away_tags", ["Let's learn"])

    status = fixture_data.get('status')

    # v1.9: Use direct fields from db.py query
    home_name = fixture_data.get('home_team_name', 'Home')
    away_name = fixture_data.get('away_team_name', 'Away')
    home_crest = fixture_data.get('home_team_crest')
    away_crest = fixture_data.get('away_team_crest')
    home_score = fixture_data.get('home_score')
    away_score = fixture_data.get('away_score')
    
    # Winner determination
    winner = None
    if status in ['FT', 'AET', 'PEN', 'FINISHED'] and home_score is not None and away_score is not None:
        if home_score > away_score:
            winner = 'HOME_TEAM'
        elif away_score > home_score:
            winner = 'AWAY_TEAM'
        else:
            winner = 'DRAW'
    
    home_score_badge = ""
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED', 'LIVE', 'HT', 'ET', 'BREAK', 'FT', 'AET', 'PEN'] and home_score is not None:
        home_score_badge = str(home_score)

    away_score_badge = ""
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED', 'LIVE', 'HT', 'ET', 'BREAK', 'FT', 'AET', 'PEN'] and away_score is not None:
        away_score_badge = str(away_score)

    status_badge_label = ""
    status_badge_type = "secondary" 

    if status in ['IN_PLAY', 'PAUSED', 'LIVE', 'HT', 'ET', 'BREAK', 'LIVE_BREAK']:
        status_badge_label = "LIVE"
        status_badge_type = "error" 
    elif status in ['FT', 'AET', 'PEN', 'FINISHED']:
        status_badge_label = "ENDED"
        status_badge_type = "info" 
    elif status in ['SUSPENDED', 'POSTPONED', 'CANCELLED', 'CANCELED', 'PST']:
        status_badge_label = "PPD"
        status_badge_type = "warning"
    else: # SCHEDULED / TIMED / TIME / NS
        _, time_gmt1 = parse_utc_to_gmt1(fixture_data.get('utc_date'))
        status_badge_label = time_gmt1[:5] # Show HH:MM
        status_badge_type = "secondary"
    
    with fixture_card:
        winner_check = winner
        if status == 'FINISHED' or status in ['FT', 'AET', 'PEN']:
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


        if status == 'FINISHED' or status in ['FT', 'AET', 'PEN']:
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
        
        st.button("Fixture Details", key=f"details_{fixture_data['fixture_id']}", 
                          on_click=open_fixture_details, args=(fixture_data,), use_container_width=True)


def open_fixture_details(fixture: Dict[str, Any]):
    st.session_state.selected_fixture = fixture
def open_league_page(league_id, league_name):
    st.session_state.view = ("league", league_id, league_name)
    st.session_state.selected_fixture = None
    st.rerun()
def open_team_page(team_id, team_name):
    st.session_state.view = ("team", team_id, team_name)
    st.session_state.selected_fixture = None
    st.rerun()


# --- Fixture Details Page (Updated) ---
def show_fixture_details(fixture: Dict[str, Any]):
    if st.button("←"):
        st.session_state.selected_fixture = None
        if 'last_view' in st.session_state:
            st.session_state.view = st.session_state.last_view
            del st.session_state['last_view']
        else:
            st.session_state.view = None 
        st.rerun()
    
    # --- v1.9: Load prediction data from the start ---
    prediction = fixture.get('prediction_data') or {}
    
    # --- v1.9: Get data directly from fixture object (no raw_data) ---
    home_name = fixture.get('home_team_name', "Home Team")
    home_crest = fixture.get('home_team_crest')
    home_team_id = fixture.get('home_team_id') 
    
    away_name = fixture.get('away_team_name', "Away Team")
    away_crest = fixture.get('away_team_crest')
    away_team_id = fixture.get('away_team_id') 
    
    league_crest = fixture.get('competition_crest')
    country_name = fixture.get('competition_country', "Unknown Country")
    league = fixture.get('competition_name', "Unknown League")
    
    # Status long may not be present, fallback to full status short text
    stage = fixture.get('status_long', fixture.get('status', 'N/A')) 
    
    home_score = fixture.get('home_score')
    away_score = fixture.get('away_score')
    
    date_gmt1, time_gmt1 = parse_utc_to_gmt1(fixture.get('utc_date'))
    date_time = f"{date_gmt1} {time_gmt1[:5]}"
    
    league_id = fixture.get('competition_code')
    if not league:
        league = league_id

    status = fixture.get('status')
    home_score_display = "-"
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED', 'FT', 'AET', 'PEN'] and home_score is not None:
        home_score_display = str(home_score)

    away_score_display = "-"
    if status in ['IN_PLAY', 'PAUSED', 'FINISHED', 'FT', 'AET', 'PEN'] and away_score is not None:
        away_score_display = str(away_score)

    status_badge_label = ""
    status_badge_type = "secondary" 
    if status in ['IN_PLAY', 'PAUSED', 'LIVE', 'HT', 'ET', 'BREAK']:
        status_badge_label = "LIVE"
        status_badge_type = "error" 
    elif status in ['FT', 'AET', 'PEN', 'FINISHED']:
        status_badge_label = "ENDED"
        status_badge_type = "info" 
    elif status in ['SUSPENDED', 'POSTPONED', 'CANCELLED', 'CANCELED', 'PST']:
        status_badge_label = "PPD"
        status_badge_type = "warning"
    else: # SCHEDULED / TIMED / TIME / NS
        status_badge_label = "SCHEDULED"
        status_badge_type = "secondary"

    # --- LEAGUE HEADER ---
    with st.container(horizontal=True, vertical_alignment="center"): 
        if league_crest:
            st.image(league_crest, width=40) 
        else:
            st.markdown("🌐")
            
        st.button(f"{country_name} : {league} - {stage}", 
                      on_click=open_league_page, 
                      args=(league_id, league), width="stretch")
            
    # --- HEADER WITH CRESTS & SCORE ---
    with st.container(horizontal=True, vertical_alignment="center"): 
        date_time_style = "font-size: 0.8em; font-weight: 500; text-align: center; margin-top: 0px;"
        st.markdown(f"<div style='{date_time_style}'>{date_time}</div>", unsafe_allow_html=True)
        
        
    with st.container(horizontal=True, vertical_alignment="top"): 
        with st.container(vertical_alignment="center", horizontal_alignment="center"): 
            if home_crest:
                st.image(home_crest, width=64)
            st.button(home_name, on_click=open_team_page, args=(home_team_id, home_name), use_container_width=True,
                      disabled=(home_team_id is None))

        score_style = "font-size: 2.5em; font-weight: 700; text-align: center; margin-top: 24px;"
        st.markdown(f"<div style='{score_style}'>{home_score_display} - {away_score_display}</div>", unsafe_allow_html=True)
        
        with st.container(vertical_alignment="center", horizontal_alignment="center"): 
            if away_crest:
                st.image(away_crest, width=64)
            st.button(away_name, on_click=open_team_page, args=(away_team_id, away_name), use_container_width=True,
                      disabled=(away_team_id is None))
                      
    with st.container(horizontal=True, vertical_alignment="center", horizontal_alignment="center"):
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
    
    table_data = get_current_standing(league_id) 

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
        st.info("No current league standings found for this league in the database.")

    st.markdown("---")
    
    # --- Prediction Tags (Shows FULL TEXT) ---
    st.markdown("#### Expert Prediction Tags")
    col_p1, col_2 = st.columns(2)
    with col_p1:
        st.markdown(f"**{home_name} Analysis:**")
        for tag in prediction.get("home_tags", ["Let's learn"]):
            st.markdown(f"- {tag}")
    with col_2:
        st.markdown(f"**{away_name} Analysis:**")
        for tag in prediction.get("away_tags", ["Let's learn"]):
            st.markdown(f"- {tag}")

    st.markdown("---")
    
    # --- Last 7 Games Section ---
    st.markdown("#### Recent Form (Last 7 Games)")
    
    home_last7 = get_last_7_home_data(prediction)
    away_last7 = get_last_7_away_data(prediction)
    
    if home_last7 and away_last7:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**{home_name}**")
            display_last7_fixture_list(home_name, home_last7 or [])
            st.space("small")
        with col2:
            st.markdown(f"**{away_name}**")
            display_last7_fixture_list(away_name, away_last7 or [])
            st.space("small")
    else:
        st.info("Recent form data has not been generated for this fixture yet.")


    # --- H2H Section ---
    st.markdown("#### Head-to-Head Encounters") 
    
    h2h_data = get_h2h_data(prediction)
    
    if h2h_data:
        st.info(f"Showing last {len(h2h_data)} H2H fixtures.")
        display_h2h_fixture_list(h2h_data) 
    else:
        st.info("No Head-to-Head data available for these teams.")


def display_last7_fixture_list(team_name: str, fixture_list: list):
    if not fixture_list:
        st.info("No recent fixture data found.")
        return
    for fixture_data in fixture_list:
        ui_fixture_data = {
            "result": (
                f"{fixture_data['home_team']} {fixture_data['home_goals']}-"
                f"{fixture_data['away_goals']} {fixture_data['away_team']}"
            ),
            "competition": fixture_data.get('league_id', 'N/A'),
            "date_gmt1": fixture_data.get('date') 
        }
        
        info = get_structured_match_info(ui_fixture_data, team_name)
        with st.container(border=True):
            league_abbr = info.get('competition', ui_fixture_data.get('competition'))
            date = format_date(ui_fixture_data.get('date_gmt1').split(" ")[0].replace("-", "-"))

            st.caption(f"{date} | {league_abbr}")
            
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


def display_h2h_fixture_list(fixture_list: list):
    if not fixture_list:
        st.info("No H2H data.")
        return
    for fixture_data in fixture_list:
        ui_fixture_data = {
            "result": (
                f"{fixture_data['home_team']} {fixture_data['home_goals']}-"
                f"{fixture_data['away_goals']} {fixture_data['away_team']}"
            ),
            "competition": fixture_data.get('league_id', 'N/A'),
            "date_gmt1": fixture_data.get('date')
        }

        info = get_structured_match_info(ui_fixture_data, "") 
        with st.container(border=True):
            league_abbr = info.get('competition', ui_fixture_data.get('competition'))
            date = format_date(ui_fixture_data.get('date_gmt1').split(" ")[0].replace("-", "-"))
            
            st.caption(f"{date} | {league_abbr}")
            
            html = f"""
            <div style='display:flex; align-items:center; justify-content:space-between; width:100%;'>
                <div style='flex:1; text-align:right; padding-right:10px;'>{info['team1_name']}</div>
                <div style='font-weight:700;'>{info['team1_score']} - {info['team2_score']}</div>
                <div style='flex:1; text-align:left; padding-left:10px;'>{info['team2_name']}</div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)