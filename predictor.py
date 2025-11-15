# predictor.py v1.17 - Rule-Based Football Predictor & Data Packager
"""
WHAT'S NEW (v1.17):
- **FIX (ON CONFLICT):** Modified the UPSERT SQL in `store_predictions_db` to explicitly use `ON CONFLICT (fixture_id)`.
  This resolves the 'no unique constraint matching the ON CONFLICT specification' error, assuming
  a unique index/constraint is applied to the `predictions.fixture_id` column in the database schema.
- INCREMENTAL SAVE: Implemented progress saving by committing the prediction batch
  to the database after every 100 fixtures processed.
- RETAINED: All v1.16 progress logging, schema adaptations, and logic.
"""

import os 
import time 
import logging 
import psycopg2 
import datetime as dt 
import json 
import argparse 
import sys 
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv 
from typing import List, Dict, Any, Optional
from datetime import timedelta, timezone

# Import database utilities (db_utils must be in the same directory)
import db_utils

# ============ CONFIG & LOGGING ============
load_dotenv() 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime objects."""
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

# --- Config ---
VERSION = "v1.17"
PREDICTION_DAYS_AHEAD = 14
CURRENT_DATE = dt.datetime.now(tz=timezone.utc)
TEN_YEARS_AGO = CURRENT_DATE - timedelta(days=365 * 10)
BATCH_COMMIT_SIZE = 100 # v1.16: Commit every 100 predictions

# Tag mapping for generating full tag strings from prediction codes
TAG_MAP = { 
    "SNG": "Score no goal", 
    "S1+": "Score At least a goal", 
    "S2+": "Score At least 2 goals", 
    "S3+": "Score At least 3 or more goals", 
    "CS": "Concede no goal", 
    "C1+": "Concede At least a goal", 
    "C2+": "Concede At least 2 goals", 
    "C3+": "Concede At least 3 or more goals", 
    "W": "Win", 
    "D": "Draw", 
    "L": "Loss", 
    "BST": "Beats Strong Teams", 
    "LWT": "Loses to Weak Teams", 
    "H2H": "H2H Dominance", 
    "T/B": "Top vs Bottom", 
    "Rival": "Close Rivals", 
}

# ============ DB UTILITIES ============

def get_fixtures_to_predict(conn, fixture_ids: Optional[List[int]]) -> List[Dict[str, Any]]: 
    """ 
    Fetches scheduled matches. If fixture_ids is provided, limits the query to those IDs.
    
    Fixtures to predict are those that: 
    1. Have status 'NS' (Not Started) or 'TBD' (To Be Defined). 
    2. Are scheduled within the next N days (if running full scan). 
    3. Have NOT been predicted yet, OR the existing prediction is OLDER than the fixture date (i.e., new result data is available).
    """ 
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query_condition = ""
    query_params = []
    
    if fixture_ids:
        # Use fixture_ids provided by sync.py trigger
        query_condition = "f.fixture_id IN %s"
        query_params.append(tuple(fixture_ids))
        logging.info(f"Running targeted scan for {len(fixture_ids)} fixture IDs.")
    else:
        # Run full scan for all relevant upcoming matches
        future_date = CURRENT_DATE + timedelta(days=PREDICTION_DAYS_AHEAD)
        logging.info(f"Running full scan. Fetching all upcoming fixtures (NS/TBD) until {future_date.strftime('%Y-%m-%d')}.")
        query_condition = f"""
            f.date >= %s AND f.date <= %s
            AND f.status_short IN ('NS', 'TBD')
            AND (
                p.fixture_id IS NULL OR p.generated_at < f.date
            )
        """
        query_params.extend([CURRENT_DATE, future_date])

    # Base query template
    query = f"""
        SELECT 
            f.fixture_id, 
            f.home_team_id, 
            f.away_team_id, 
            f.league_id,
            f.season_year,
            -- Prediction details (to check staleness)
            p.generated_at AS last_prediction_at
        FROM 
            fixtures f
        LEFT JOIN 
            predictions p ON f.fixture_id = p.fixture_id
        WHERE 
            {query_condition}
        ORDER BY 
            f.date ASC
    """
    
    cursor.execute(query, tuple(query_params))
    rows = cursor.fetchall()
    cursor.close()
    return rows

def get_standings(conn, league_id: int) -> Dict[int, int]: 
    """ 
    Fetches current points for teams in the league from the latest season_year. 
    Returns {team_id: points}. 
    """ 
    cursor = conn.cursor(cursor_factory=RealDictCursor) 
    query = """ 
        WITH latest_season AS ( 
            SELECT MAX(season_year) as max_year 
            FROM standings 
            WHERE league_id = %s 
        ) 
        SELECT s.team_id, s.points 
        FROM standings s 
        JOIN latest_season ls ON s.season_year = ls.max_year 
        WHERE s.league_id = %s
    """ 
    cursor.execute(query, (league_id, league_id)) 
    rows = cursor.fetchall() 
    cursor.close() 
    return {row['team_id']: row['points'] for row in rows} if rows else {}

def get_historical_matches(conn, team_id: int, league_id: int, ten_years_ago: dt.datetime, limit: int = 10) -> List[Dict[str, Any]]: 
    """ 
    Fetches the last N completed (FT) matches for a team, filtered to past 10 years and same league. 
    """ 
    cursor = conn.cursor(cursor_factory=RealDictCursor) 
    query = """ 
        SELECT 
            f.date, 
            f.home_team_id, 
            f.away_team_id, 
            f.goals_home, 
            f.goals_away, 
            f.status_short 
        FROM 
            fixtures f 
        WHERE 
            (f.home_team_id = %s OR f.away_team_id = %s) 
            AND f.status_short = 'FT' 
            AND f.league_id = %s 
            AND f.date >= %s 
        ORDER BY 
            f.timestamp DESC 
        LIMIT %s
    """ 
    cursor.execute(query, (team_id, team_id, league_id, ten_years_ago, limit)) 
    matches = cursor.fetchall() 
    cursor.close() 
    return matches

def get_h2h_matches_all(conn, team_a_id: int, team_b_id: int, ten_years_ago: dt.datetime, limit: int = 10) -> List[Dict[str, Any]]: 
    """ 
    Fetches all Head-to-Head completed matches (both venues) for UI packaging. 
    """ 
    cursor = conn.cursor(cursor_factory=RealDictCursor) 
    query = """ 
        SELECT 
            f.date, 
            f.goals_home, 
            f.goals_away, 
            ht.name AS home_team_name, 
            at.name AS away_team_name 
        FROM 
            fixtures f 
        JOIN 
            teams ht ON f.home_team_id = ht.team_id 
        JOIN 
            teams at ON f.away_team_id = at.team_id 
        WHERE 
            ((f.home_team_id = %s AND f.away_team_id = %s) OR (f.home_team_id = %s AND f.away_team_id = %s)) 
            AND f.status_short = 'FT' 
            AND f.date >= %s 
        ORDER BY 
            f.timestamp DESC 
        LIMIT %s
    """ 
    cursor.execute(query, (team_a_id, team_b_id, team_b_id, team_a_id, ten_years_ago, limit)) 
    matches = cursor.fetchall() 
    cursor.close() 
    return matches

def get_h2h_matches_venue(conn, team_a_id: int, team_b_id: int, is_home: bool, league_id: int, ten_years_ago: dt.datetime) -> List[Dict[str, Any]]: 
    """ 
    Fetches venue-specific Head-to-Head completed matches for algorithm, filtered to same league. 
    """ 
    if is_home: 
        home_id, away_id = team_a_id, team_b_id 
    else: 
        # Note: This case is generally not used for team_a_id if it's the 'away' team 
        # but the query structure requires it. We typically look at H2H from the perspective of team_a_id's role.
        home_id, away_id = team_b_id, team_a_id
        
    cursor = conn.cursor(cursor_factory=RealDictCursor) 
    query = """ 
        SELECT 
            f.date, 
            f.home_team_id, 
            f.away_team_id, 
            f.goals_home, 
            f.goals_away 
        FROM 
            fixtures f 
        WHERE 
            f.home_team_id = %s 
            AND f.away_team_id = %s 
            AND f.status_short = 'FT' 
            AND f.league_id = %s 
            AND f.date >= %s 
        ORDER BY 
            f.timestamp DESC
    """ 
    cursor.execute(query, (home_id, away_id, league_id, ten_years_ago)) 
    matches = cursor.fetchall() 
    cursor.close() 
    return matches

def get_similar_tier_matches(conn, team_a_id: int, opponents_in_tier: List[int], team_b_id: int, is_home: bool, league_id: int, ten_years_ago: dt.datetime) -> List[Dict[str, Any]]: 
    """ 
    Fetches matches against similar-tier opponents (excluding self-matchup), with home/away context, filtered to same league. 
    """ 
    if not opponents_in_tier: 
        return [] 
        
    cursor = conn.cursor(cursor_factory=RealDictCursor) 
    
    # We use a tuple for the IN clause
    opponents_tuple = tuple(opponents_in_tier)
    
    if is_home: 
        query = """ 
            SELECT 
                f.date, f.home_team_id, f.away_team_id, f.goals_home, f.goals_away 
            FROM 
                fixtures f 
            WHERE 
                f.home_team_id = %s 
                AND f.away_team_id IN %s 
                AND f.away_team_id != %s 
                AND f.status_short = 'FT' 
                AND f.league_id = %s 
                AND f.date >= %s 
            ORDER BY 
                f.timestamp DESC
        """ 
        cursor.execute(query, (team_a_id, opponents_tuple, team_b_id, league_id, ten_years_ago)) 
    else: 
        query = """ 
            SELECT 
                f.date, f.home_team_id, f.away_team_id, f.goals_home, f.goals_away 
            FROM 
                fixtures f 
            WHERE 
                f.away_team_id = %s 
                AND f.home_team_id IN %s 
                AND f.home_team_id != %s 
                AND f.status_short = 'FT' 
                AND f.league_id = %s 
                AND f.date >= %s 
            ORDER BY 
                f.timestamp DESC
        """ 
        cursor.execute(query, (team_a_id, opponents_tuple, team_b_id, league_id, ten_years_ago)) 
        
    matches = cursor.fetchall() 
    cursor.close() 
    return matches

def get_overall_matches(conn, team_a_id: int, team_b_id: int, is_home: bool, league_id: int, ten_years_ago: dt.datetime) -> List[Dict[str, Any]]: 
    """ 
    Fetches all contextual (home/away) matches excluding self-matchup, filtered to same league. 
    """ 
    cursor = conn.cursor(cursor_factory=RealDictCursor) 
    if is_home: 
        query = """ 
            SELECT 
                f.date, f.home_team_id, f.away_team_id, f.goals_home, f.goals_away 
            FROM 
                fixtures f 
            WHERE 
                f.home_team_id = %s 
                AND f.away_team_id != %s 
                AND f.status_short = 'FT' 
                AND f.league_id = %s 
                AND f.date >= %s 
            ORDER BY 
                f.timestamp DESC
        """ 
        cursor.execute(query, (team_a_id, team_b_id, league_id, ten_years_ago)) 
    else: 
        query = """ 
            SELECT 
                f.date, f.home_team_id, f.away_team_id, f.goals_home, f.goals_away 
            FROM 
                fixtures f 
            WHERE 
                f.away_team_id = %s 
                AND f.home_team_id != %s 
                AND f.status_short = 'FT' 
                AND f.league_id = %s 
                AND f.date >= %s 
            ORDER BY 
                f.timestamp DESC
        """ 
        cursor.execute(query, (team_a_id, team_b_id, league_id, ten_years_ago)) 
        
    matches = cursor.fetchall() 
    cursor.close() 
    return matches

def store_predictions_db(conn, predictions_list: List[Dict[str, Any]]): 
    """ 
    Inserts a batch of predictions into the 'predictions' table.
    Uses ON CONFLICT (fixture_id) DO UPDATE SET.
    """ 
    if not predictions_list: 
        logging.info("No predictions generated to store.") 
        return
        
    cursor = conn.cursor() 
    data_to_insert = [] 
    current_time = CURRENT_DATE 
    
    for pred in predictions_list: 
        # v1.17: Store fixture_id, prediction_data (JSON), generated_at
        data_to_insert.append(( 
            pred['fixture_id'], 
            json.dumps(pred['predictions'], cls=DateTimeEncoder), 
            current_time 
        ))

    insert_sql = """
        INSERT INTO predictions (fixture_id, prediction_data, generated_at)
        VALUES %s
        ON CONFLICT (fixture_id) DO UPDATE SET
            prediction_data = EXCLUDED.prediction_data,
            generated_at = EXCLUDED.generated_at;
    """
    
    try:
        execute_values(cursor, insert_sql, data_to_insert)
        conn.commit()
        logging.info(f"Successfully stored/updated {len(predictions_list)} predictions.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to store predictions: {e}")
        raise # Re-raise the exception to stop the main process if a critical DB error occurs


# ============ PREDICTION LOGIC (Updated Rule-Based) ============

def get_tier(points: int) -> str: 
    """ Computes team tier based on current points. """ 
    if points >= 60: 
        return 'high' 
    elif points >= 40: 
        return 'mid' 
    else: 
        return 'low'

def is_win(match: Dict[str, Any], team_id: int) -> bool: 
    goals_scored = get_team_goals(match, team_id) 
    goals_conceded = get_team_conceded(match, team_id) 
    return goals_scored > goals_conceded

def is_draw(match: Dict[str, Any], team_id: int) -> bool: 
    goals_scored = get_team_goals(match, team_id) 
    goals_conceded = get_team_conceded(match, team_id) 
    return goals_scored == goals_conceded

def is_loss(match: Dict[str, Any], team_id: int) -> bool: 
    goals_scored = get_team_goals(match, team_id) 
    goals_conceded = get_team_conceded(match, team_id) 
    return goals_scored < goals_conceded

def get_team_goals(match: Dict[str, Any], team_id: int) -> int: 
    if match['home_team_id'] == team_id: 
        return match['goals_home'] or 0 
    elif match['away_team_id'] == team_id: 
        return match['goals_away'] or 0 
    return 0

def get_team_conceded(match: Dict[str, Any], team_id: int) -> int: 
    return get_team_goals(match, opponent_of(match, team_id))

def opponent_of(match: Dict[str, Any], team_id: int) -> int: 
    return match['away_team_id'] if match['home_team_id'] == team_id else match['home_team_id']

def get_opponent_tier(match: Dict[str, Any], team_id: int, standings: Dict[int, int]) -> str: 
    opp_id = opponent_of(match, team_id) 
    points = standings.get(opp_id, 0) 
    return get_tier(points)

def predict_for_team( 
    conn, 
    team_a_id: int, 
    team_b_id: int, 
    is_home: bool, 
    league_id: int, 
    standings: Dict[int, int] 
) -> Dict[str, bool]: 
    """ Generates predictions for a single team using the updated algorithm. """ 
    tier_a = get_tier(standings.get(team_a_id, 0)) 
    tier_b = get_tier(standings.get(team_b_id, 0))
    
    # --- 1. Rule-Based Attributes (T/B, Rival) ---
    attributes = { 
        'T/B': (tier_a == 'high' and tier_b == 'low') or (tier_a == 'low' and tier_b == 'high'), 
        'Rival': abs(standings.get(team_a_id, 0) - standings.get(team_b_id, 0)) <= 5 
    }

    # --- 2. Historical Data Fetch ---
    # Last 7 for Recent Form visualization
    last_7_matches = get_historical_matches(conn, team_a_id, league_id, TEN_YEARS_AGO, limit=7)
    
    # Overall matches in context (home/away, excluding this opponent)
    overall_context_matches = get_overall_matches(conn, team_a_id, team_b_id, is_home, league_id, TEN_YEARS_AGO)
    
    # H2H matches in context (venue-specific)
    h2h_context_matches = get_h2h_matches_venue(conn, team_a_id, team_b_id, is_home, league_id, TEN_YEARS_AGO)

    # Similar tier opponents (for W/L analysis)
    all_teams_in_league = list(standings.keys())
    opponents_in_tier = [
        tid for tid in all_teams_in_league 
        if get_tier(standings.get(tid, 0)) == tier_b
    ]
    similar_tier_matches = get_similar_tier_matches(conn, team_a_id, opponents_in_tier, team_b_id, is_home, league_id, TEN_YEARS_AGO)

    # --- 3. Compute Metrics ---
    
    # Win/Loss/Draw Count
    recent_wins = sum(1 for match in last_7_matches if is_win(match, team_a_id))
    recent_draws = sum(1 for match in last_7_matches if is_draw(match, team_a_id))
    
    # Goal Metrics (Overall Contextual)
    overall_goals_scored = sum(get_team_goals(match, team_a_id) for match in overall_context_matches)
    overall_goals_conceded = sum(get_team_conceded(match, team_a_id) for match in overall_context_matches)
    overall_played = len(overall_context_matches) or 1
    
    avg_scored = overall_goals_scored / overall_played
    avg_conceded = overall_goals_conceded / overall_played
    
    # Strength/Weakness vs Tier (for BST/LWT)
    high_tier_matches = [
        match for match in last_7_matches if get_opponent_tier(match, team_a_id, standings) == 'high'
    ]
    low_tier_matches = [
        match for match in last_7_matches if get_opponent_tier(match, team_a_id, standings) == 'low'
    ]
    
    high_tier_wins = sum(1 for match in high_tier_matches if is_win(match, team_a_id))
    low_tier_losses = sum(1 for match in low_tier_matches if is_loss(match, team_a_id))
    
    # H2H Dominance Check
    h2h_wins = sum(1 for match in h2h_context_matches if is_win(match, team_a_id))
    h2h_losses = sum(1 for match in h2h_context_matches if is_loss(match, team_a_id))
    
    # --- 4. Generate Predictions (True/False) ---
    predictions = {
        # Win/Draw/Loss based on recent form vs similar tier
        "W": recent_wins >= 4,
        "D": recent_draws >= 3,
        "L": low_tier_losses >= 2,
        
        # Goal predictions based on averages
        "S1+": avg_scored >= 1.0,
        "S2+": avg_scored >= 1.5,
        "S3+": avg_scored >= 2.5,
        "CS": avg_conceded < 0.5,
        "C1+": avg_conceded >= 1.0,
        "C2+": avg_conceded >= 1.5,
        "C3+": avg_conceded >= 2.5,
        
        # Specialist tags
        "BST": high_tier_wins >= 2,
        "LWT": low_tier_losses >= 2,
        "H2H": h2h_wins > h2h_losses and len(h2h_context_matches) >= 3,
        
        # Attributes
        "T/B": attributes['T/B'],
        "Rival": attributes['Rival'],
    }
    
    # Package data for UI
    ui_data = {
        'W': predictions['W'], 'D': predictions['D'], 'L': predictions['L'],
        'S1+': predictions['S1+'], 'S2+': predictions['S2+'], 'S3+': predictions['S3+'],
        'CS': predictions['CS'], 'C1+': predictions['C1+'], 'C2+': predictions['C2+'], 'C3+': predictions['C3+'],
        'BST': predictions['BST'], 'LWT': predictions['LWT'], 'H2H': predictions['H2H'],
        'T/B': predictions['T/B'], 'Rival': predictions['Rival'],
        
        # Raw data for UI visualization
        'last7': last_7_matches,
        'avg_scored': round(avg_scored, 2),
        'avg_conceded': round(avg_conceded, 2),
    }

    return ui_data

def generate_tags(predictions: Dict[str, bool]) -> List[str]: 
    """ Converts True predictions to full tag strings using TAG_MAP. """ 
    tags = [] 
    for code, full_tag in TAG_MAP.items(): 
        # Check against both rule-based and attribute keys
        if predictions.get(code, False) or predictions.get(code.lower(), False):
            tags.append(full_tag) 
    return tags

def run_prediction(conn, match: Dict[str, Any]) -> Dict[str, Any]: 
    """ 
    Generates predictions and packages data for one match using the updated algorithm. 
    """ 
    home_id = match['home_team_id'] 
    away_id = match['away_team_id'] 
    league_id = match['league_id']
    
    # Fetch standings (only once per match)
    standings = get_standings(conn, league_id)

    # 1. Predict for Home Team
    home_pred_raw = predict_for_team(conn, home_id, away_id, is_home=True, league_id=league_id, standings=standings)
    
    # 2. Predict for Away Team
    away_pred_raw = predict_for_team(conn, away_id, home_id, is_home=False, league_id=league_id, standings=standings)
    
    # 3. Fetch H2H for UI visualization (All venues)
    h2h_ui_data = get_h2h_matches_all(conn, home_id, away_id, TEN_YEARS_AGO, limit=10)

    # 4. Package final JSONB structure (v1.17)
    final_prediction_json = {
        # Visualization data
        "h2h": h2h_ui_data,
        "home_last7": home_pred_raw['last7'],
        "away_last7": away_pred_raw['last7'],
        
        # Consensus Outcomes (Default False)
        "home_win": home_pred_raw['W'] and away_pred_raw['L'],
        "away_win": away_pred_raw['W'] and home_pred_raw['L'],
        "draw": home_pred_raw['D'] and away_pred_raw['D'],

        # Tags and Goal Metrics
        "home_tags": generate_tags(home_pred_raw),
        "away_tags": generate_tags(away_pred_raw),

        "home_avg_scored": home_pred_raw['avg_scored'],
        "away_avg_scored": away_pred_raw['avg_scored'],
        
        # Over/Under
        "total_over_2": (home_pred_raw['avg_scored'] + away_pred_raw['avg_scored']) >= 2.5,
        "total_under_2": (home_pred_raw['avg_scored'] + away_pred_raw['avg_scored']) < 1.5,
    }
    
    # Add fallback tag
    if not final_prediction_json['home_tags']:
        final_prediction_json['home_tags'].append("Let's learn")
    if not final_prediction_json['away_tags']:
        final_prediction_json['away_tags'].append("Let's learn")

    return {
        'fixture_id': match['fixture_id'],
        'predictions': final_prediction_json
    }

# ============ MAIN EXECUTION ============

def main(): 
    parser = argparse.ArgumentParser(description="Rule-Based Football Predictor.") 
    parser.add_argument("--fixtures", type=str, default=None, help="Comma-separated list of fixture_ids to predict.") 
    args = parser.parse_args()
    
    # Process fixture IDs from argument
    fixture_ids_to_predict: Optional[List[int]] = None
    if args.fixtures:
        try:
            fixture_ids_to_predict = [int(x.strip()) for x in args.fixtures.split(',') if x.strip()]
        except ValueError:
            logging.error("Invalid fixture ID list provided. Aborting.")
            sys.exit(1)
            
    conn = None 
    try: 
        db_utils.init_connection_pool() 
        conn = db_utils.get_connection()

        if conn is None:
            logging.error("Failed to acquire database connection.")
            sys.exit(1)
            
        # 1. Fetch matches requiring prediction
        matches_to_predict = get_fixtures_to_predict(conn, fixture_ids_to_predict)
        
        if not matches_to_predict:
            logging.info("No fixtures found requiring prediction/update.")
            return

        logging.info(f"Predictor {VERSION} found {len(matches_to_predict)} fixtures to predict.")

        # 2. Run prediction cycle
        all_predictions_to_store: List[Dict[str, Any]] = []
        
        for i, match in enumerate(matches_to_predict):
            try:
                prediction_data = run_prediction(conn, match)
                all_predictions_to_store.append(prediction_data)
                
                # v1.16: Incremental Save Logic
                if (i + 1) % BATCH_COMMIT_SIZE == 0:
                    logging.info(f"Processed {i + 1}/{len(matches_to_predict)} fixtures. Saving batch to database...")
                    # Store and immediately clear the buffer
                    store_predictions_db(conn, all_predictions_to_store)
                    all_predictions_to_store = []
                    
            except Exception as e:
                logging.error(f"Failed to process fixture {match['fixture_id']}: {e}")
                # Continue to next fixture, preserving the overall batch integrity
                
        # 3. Store any remaining predictions in the final batch
        if all_predictions_to_store:
            logging.info(f"Processing final batch of {len(all_predictions_to_store)} predictions. Saving to database...")
            store_predictions_db(conn, all_predictions_to_store)

    except Exception as e:
        logging.error(f"Predictor main process failed: {e}")
        if conn:
            conn.rollback() # Ensure rollback on failure
    finally:
        if conn:
            db_utils.release_connection(conn)
        # Note: db_utils handles closing the pool globally
        
    logging.info("Predictor script finished.")


if __name__ == "__main__": 
    main()