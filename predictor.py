# predictor.py v1.4 - Rule-Based Football Predictor & Data Packager
#
# WHAT'S NEW (v1.4):
# - BUG FIX (MAJOR): This script is now responsible for populating
#   Head-to-Head (H2H) and Recent Form (Last 7) data.
# - DATA PACKAGING: H2H and Recent Form lists are now calculated
#   and saved *inside* the prediction_data JSONB.
# - UI FIX: This provides the missing data that widgets.py (v1.4)
#   expects, fixing the empty H2H/Recent Form sections in the UI.

import os
import time
import logging
import psycopg2
import datetime as dt
import json
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

# ============ CONFIG & LOGGING ============
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Config ---
VERSION = "v1.4"  # <-- NEW VERSION VARIABLE
# This remains 14 days as requested by the user's prediction scope rule.
PREDICTION_DAYS_AHEAD = 14
GOAL_THRESHOLDS = [1, 2, 3, 4]

# ============ CONNECT ============
try:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.error("DATABASE_URL not found. Check .env file or Streamlit Secrets.")
        exit(1)

    if db_url.startswith("postgresql+psycopg://"):
        db_url = db_url.replace("postgresql+psycopg://", "postgresql://", 1)

    db_pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        dsn=db_url,
        cursor_factory=RealDictCursor
    )
    logging.info("Database connection pool created.")
except Exception as e:
    logging.error(f"DB connection pool failed: {e}")
    exit(1)


# --- Helper Functions (Core Logic from User Prompt) ---

def parse_date(date_str: str) -> dt.date:
    """Helper function to convert date string to date object."""
    try:
        return dt.datetime.strptime(date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        logging.warning(f"Could not parse date: {date_str}. Using today.")
        return dt.date.today()

def get_team_goals(m: Dict, team: str) -> int:
    """Get goals scored by the specified team in a match."""
    return m['home_goals'] if m['home_team'] == team else m['away_goals']

def get_team_conceded(m: Dict, team: str) -> int:
    """Get goals conceded by the specified team."""
    return get_team_goals(m, opponent_of(m, team))

def opponent_of(m: Dict, team: str) -> str:
    """Get the opponent team's name."""
    return m['away_team'] if m['home_team'] == team else m['home_team']

def is_win(m: Dict, team: str) -> bool:
    """Check if the team won the match."""
    return get_team_goals(m, team) > get_team_conceded(m, team)

def is_loss(m: Dict, team: str) -> bool:
    """Check if the team lost the match."""
    return get_team_goals(m, team) < get_team_conceded(m, team)
    
def is_draw(m: Dict) -> bool:
    """Check if the match was a draw."""
    return m['home_goals'] == m['away_goals']
    
def count_events(matches_subset: List[Dict], event_condition) -> int:
    """Counts the number of matches that satisfy a given condition."""
    return sum(1 for m in matches_subset if event_condition(m))

def calculate_all_team_tiers(all_matches: List[Dict]) -> Dict[str, str]:
    """Determines the historical tier of all teams based on absolute win count."""
    logging.info("Calculating historical tiers for all teams...")
    team_wins = {}
    for m in all_matches:
        if is_win(m, m['home_team']):
            team_wins[m['home_team']] = team_wins.get(m['home_team'], 0) + 1
        elif is_win(m, m['away_team']):
            team_wins[m['away_team']] = team_wins.get(m['away_team'], 0) + 1
            
    team_tiers = {}
    for team, wins in team_wins.items():
        if wins > 100:
            team_tiers[team] = 'high'
        elif wins > 50:
            team_tiers[team] = 'mid'
        else:
            team_tiers[team] = 'low'
    
    logging.info(f"Calculated tiers for {len(team_tiers)} teams.")
    return team_tiers

# --- Prediction Core Function (from User Prompt) ---

def predict_for_team(
    team_a: str, 
    team_b: str, 
    is_home: bool, 
    all_matches: List[Dict], 
    current_date: dt.date,
    team_tiers: Dict[str, str] # Pass in pre-calculated tiers
) -> Dict[str, Any]:
    """
    Generates all rule-based predictions for Team A in an upcoming match.
    Strictly avoids averages or statistical means.
    """
    
    # Step 1: Filter data to past 10 years
    ten_years_ago = current_date - dt.timedelta(days=365 * 10)
    filtered_matches = [m for m in all_matches if parse_date(m['date']) >= ten_years_ago]
    
    # --- Step 2 & 3: Filter and Define Subsets ---
    
    # Head-to-head (H2H)
    # v1.4: H2H logic now matches UI (is_home doesn't matter for H2H)
    h2h_matches_all = sorted([m for m in filtered_matches if 
                           (m['home_team'] == team_a and m['away_team'] == team_b) or
                           (m['home_team'] == team_b and m['away_team'] == team_a)],
                           key=lambda m: parse_date(m['date']), reverse=True)
    
    # Use only H2H matches *in context* (home/away) for *prediction rules*
    h2h_matches_context = [m for m in h2h_matches_all if
                           (m['home_team'] == team_a and is_home) or
                           (m['away_team'] == team_a and not is_home)]
    
    # Recent form (last 10 matches for team_a)
    team_a_matches_all = sorted([m for m in filtered_matches if team_a in (m['home_team'], m['away_team'])], 
                                key=lambda m: parse_date(m['date']), reverse=True)
    team_a_recent_matches = team_a_matches_all[:10]
    
    # Opponent tier matches
    tier = team_tiers.get(team_b, 'low')
    similar_opponent_matches = [m for m in filtered_matches if 
                                opponent_of(m, team_a) != team_b and
                                team_tiers.get(opponent_of(m, team_a), 'low') == tier and
                                (m['home_team'] == team_a if is_home else m['away_team'] == team_a)]
    
    # Overall (all team_a matches in context)
    overall_matches = [m for m in filtered_matches if 
                       (m['home_team'] == team_a if is_home else m['away_team'] == team_a)]
    
    # Hierarchy of subsets (prefer more specific)
    # v1.4: Use h2h_matches_context for rules
    subsets_priority = [h2h_matches_context, team_a_recent_matches, similar_opponent_matches, overall_matches]
    
    # Define all prediction types (expanded)
    pred_types = []
    for g in GOAL_THRESHOLDS:
        pred_types.append(f'score_{g}')
        pred_types.append(f'concede_{g}')
        pred_types.append(f'total_over_{g}')
        pred_types.append(f'total_under_{g}')
        
    pred_types.extend(['win', 'loss', 'draw'])
    predictions: Dict[str, bool] = {}
    
    # --- Step 4: Apply Rules and Thresholds ---
    
    for pred_type in pred_types:
        for subset in subsets_priority:
            if len(subset) < 5:
                continue
            
            count = 0
            is_prediction_set = False
            threshold = 0
            
            # 1. Team A Score/Concede Goals
            if pred_type.startswith('score_') or pred_type.startswith('concede_'):
                goal_target = int(pred_type.split('_')[-1])
                is_score = pred_type.startswith('score_')
                
                if is_score:
                    count = count_events(subset, lambda m: get_team_goals(m, team_a) >= goal_target)
                else: # Concede
                    count = count_events(subset, lambda m: get_team_conceded(m, team_a) >= goal_target)
                
                threshold = len(subset) // 2 + 1 if goal_target == 1 else max(3, len(subset) // 3) 
                is_prediction_set = True
                
            # 2. Total Goals Over/Under
            elif pred_type.startswith('total_over_') or pred_type.startswith('total_under_'):
                goal_target = int(pred_type.split('_')[-1])
                is_over = pred_type.startswith('total_over_')
                
                if is_over:
                    count = count_events(subset, lambda m: m['home_goals'] + m['away_goals'] > goal_target)
                else: # Under
                    count = count_events(subset, lambda m: m['home_goals'] + m['away_goals'] < goal_target)
                
                threshold = len(subset) // 2 + 1 
                is_prediction_set = True
                
            # 3. Match Outcomes (Win/Loss/Draw)
            elif pred_type in ['win', 'loss', 'draw']:
                condition = is_win if pred_type == 'win' else is_loss if pred_type == 'loss' else is_draw
                count = count_events(subset, lambda m: condition(m, team_a) if pred_type != 'draw' else is_draw(m))
                threshold = len(subset) // 2 + 1
                is_prediction_set = True

            if is_prediction_set:
                predictions[pred_type] = count >= threshold
                break
        
        if pred_type not in predictions:
            predictions[pred_type] = False 

    # --- Step 5: Consistency and Double Chance Check ---
    
    if predictions.get('win', False) and predictions.get('loss', False):
        predictions['loss'] = False
        predictions['draw'] = False

    predictions['double_chance_a_or_draw'] = predictions.get('win', False) or predictions.get('draw', False)
    predictions['double_chance_b_or_draw'] = predictions.get('loss', False) or predictions.get('draw', False)
    predictions['double_chance_a_or_b'] = (predictions.get('win', False) or predictions.get('loss', False)) and not predictions.get('draw', False) 

    # --- v1.4: Package H2H and Recent Form data ---
    # This data is for the UI, not for rules, so we use the full lists
    
    # H2H: Get top 5 most recent, regardless of home/away
    predictions['h2h'] = h2h_matches_all[:5] 
    
    # Recent Form: Get top 7 most recent
    predictions['recent_form'] = team_a_matches_all[:7]
    
    # --- End v1.4 ---

    return predictions

# --- Data Fetching and Main Execution Logic (MODIFIED) ---

def get_all_historical_data(conn) -> List[Dict[str, Any]]:
    """
    Fetches 10 years of raw historical match data from the database.
    v1.4: Also fetches competition code for UI display.
    """
    logging.info("--- Fetching All Historical Match Data (10 years) ---")
    sql = """
    SELECT
        m.utc_date::date::text AS date, -- Cast to YYYY-MM-DD string
        ht.name AS home_team,
        at.name AS away_team,
        m.score_fulltime_home AS home_goals,
        m.score_fulltime_away AS away_goals,
        c.code AS competition_code -- v1.4: Get competition code
    FROM matches m
    JOIN teams ht ON m.home_team_id = ht.team_id
    JOIN teams at ON m.away_team_id = at.team_id
    JOIN competitions c ON m.competition_id = c.competition_id -- v1.4: Join
    WHERE m.status = 'FINISHED'
      AND m.utc_date IS NOT NULL
      AND m.score_fulltime_home IS NOT NULL
      AND m.score_fulltime_away IS NOT NULL
      AND m.utc_date >= (NOW() - INTERVAL '10 years');
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        data = cur.fetchall()
    logging.info(f"Fetched {len(data)} historical matches.")
    return [dict(row) for row in data]


def get_upcoming_matches(conn, days_ahead: int) -> List[Dict[str, Any]]:
    """
    Fetches all matches scheduled (or timed) within the next N days.
    (This function ensures prediction is only for the next 14 days)
    """
    today = dt.date.today()
    future_date = today + dt.timedelta(days=days_ahead)
    logging.info(f"--- Fetching Upcoming Matches (SCHEDULED or TIMED) between {today} and {future_date} ---")
    
    sql = """
    SELECT
        m.match_id,
        m.utc_date::date::text AS date, -- Cast to YYYY-MM-DD string
        ht.name AS home_team,
        at.name AS away_team
    FROM matches m
    JOIN teams ht ON m.home_team_id = ht.team_id
    JOIN teams at ON m.away_team_id = at.team_id
    WHERE m.status IN ('SCHEDULED', 'TIMED')
      AND m.utc_date BETWEEN NOW() AND NOW() + INTERVAL %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (f'{days_ahead} days',))
        data = cur.fetchall()
    logging.info(f"Found {len(data)} upcoming matches.")
    return [dict(row) for row in data]

def store_predictions_db(conn, predictions_list: List[Dict[str, Any]]):
    """
    Stores the list of prediction dictionaries in the 'predictions' table.
    """
    if not predictions_list:
        logging.info("--- No predictions to store ---")
        return

    sql = """
    INSERT INTO predictions (match_id, prediction_data)
    VALUES %s
    ON CONFLICT (match_id) DO UPDATE SET
        prediction_data = EXCLUDED.prediction_data,
        generated_at = NOW();
    """
    
    values = [
        (
            p['match_id'],
            json.dumps(p['predictions']) # Store the predictions dict as JSONB
        )
        for p in predictions_list
    ]
    
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=len(values))
        conn.commit()
        logging.info(f"--- Successfully stored/updated {len(values)} predictions in the database ---")
    except Exception as e:
        logging.error(f"--- ERROR storing predictions: {e} ---")
        conn.rollback()


def main():
    """Main execution flow for the predictor script."""
    logging.info(f"--- PREDICTOR (v{VERSION}) STARTING ---")
    start_time = time.time()
    current_date = dt.date.today()
    conn = None
    
    try:
        conn = db_pool.getconn()
        
        # 1. Get all necessary data
        historical_data = get_all_historical_data(conn)
        # This uses PREDICTION_DAYS_AHEAD (14)
        upcoming_matches = get_upcoming_matches(conn, days_ahead=PREDICTION_DAYS_AHEAD) 
        
        if not historical_data:
            logging.error("ERROR: No historical data available. Cannot run predictions.")
            return

        team_tiers = calculate_all_team_tiers(historical_data)

        # 2. Generate predictions for all upcoming matches
        all_predictions_to_store = []
        
        if not upcoming_matches:
            logging.info("No upcoming matches found to predict.")
        else:
            logging.info(f"Generating predictions for {len(upcoming_matches)} matches...")
        
        for match in upcoming_matches:
            home_team = match['home_team']
            away_team = match['away_team']
            
            logging.info(f"\n--- Predicting: {home_team} vs {away_team} on {match['date']} ---")
            
            # --- v1.4: predict_for_team now returns all data ---
            pred_a = predict_for_team(
                home_team, away_team, is_home=True, 
                all_matches=historical_data, current_date=current_date, team_tiers=team_tiers
            )
            
            pred_b = predict_for_team(
                away_team, home_team, is_home=False, 
                all_matches=historical_data, current_date=current_date, team_tiers=team_tiers
            )

            # --- v1.4: Build the final JSONB object ---
            final_prediction_json = {
                # --- Rule Tags ---
                'home_tags': [], # We will build this from rules
                'away_tags': [], # We will build this from rules
                
                # --- H2H & Recent Form Data ---
                'h2h': pred_a['h2h'], # Use pred_a's H2H (it's symmetrical)
                'home_last7': pred_a['recent_form'],
                'away_last7': pred_b['recent_form'],
                
                # --- Raw Rules (for potential future use, optional) ---
                'home_win': pred_a['win'],
                'away_win': pred_b['win'],
                'draw': pred_a['draw'],
                'home_score_1': pred_a['score_1'],
                'away_score_1': pred_b['score_1'],
                'home_score_2': pred_a['score_2'],
                'away_score_2': pred_b['score_2'],
                'home_concede_1': pred_a['concede_1'],
                'away_concede_1': pred_b['concede_1'],
                'total_over_2': pred_a['total_over_2'],
                'total_under_2': pred_a['total_under_2'],
            }
            
            # --- v1.4: Populate 'home_tags' and 'away_tags' ---
            # (This is the logic that was missing)
            if pred_a['win']: final_prediction_json['home_tags'].append("Win")
            if pred_a['loss']: final_prediction_json['home_tags'].append("Loss")
            if pred_a['draw']: final_prediction_json['home_tags'].append("Draw")
            if pred_a['score_1']: final_prediction_json['home_tags'].append("Score At least a goal")
            if pred_a['score_2']: final_prediction_json['home_tags'].append("Score At least 2 goals")
            if pred_a['concede_1']: final_prediction_json['home_tags'].append("Concede At least a goal")
            
            if pred_b['win']: final_prediction_json['away_tags'].append("Win")
            if pred_b['loss']: final_prediction_json['away_tags'].append("Loss")
            if pred_b['draw']: final_prediction_json['away_tags'].append("Draw")
            if pred_b['score_1']: final_prediction_json['away_tags'].append("Score At least a goal")
            if pred_b['score_2']: final_prediction_json['away_tags'].append("Score At least 2 goals")
            if pred_b['concede_1']: final_prediction_json['away_tags'].append("Concede At least a goal")
            
            # Add fallback tag
            if not final_prediction_json['home_tags']:
                final_prediction_json['home_tags'].append("Let's learn")
            if not final_prediction_json['away_tags']:
                final_prediction_json['away_tags'].append("Let's learn")
            
            # Add the complete JSONB to the list to be stored
            all_predictions_to_store.append({
                'match_id': match['match_id'],
                'predictions': final_prediction_json
            })
            # --- End v1.4 packaging logic ---

        # 3. Store predictions
        store_predictions_db(conn, all_predictions_to_store)

    except Exception as e:
        logging.error(f"Predictor main process failed: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            db_pool.putconn(conn)
        db_pool.closeall()
        end_time = time.time()
        logging.info(f"Predictor script finished. Total runtime: {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()