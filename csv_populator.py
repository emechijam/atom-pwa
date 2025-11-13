# csv_populator.py v1.13

import os
import csv
import sys
import logging
import datetime
import pytz
import re
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.errors import IntegrityError

# Optional dependency for robust date parsing
try:
    from dateutil import parser as date_parser # type: ignore
    HAS_DATEUTIL = True
except Exception:
    HAS_DATEUTIL = False

# ============ CONFIG & LOGGING ============
from dotenv import load_dotenv
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# DB Config from .env (required)
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

# Validate env
missing = [k for k, v in (("DB_HOST", DB_HOST), ("DB_NAME", DB_NAME), ("DB_USER", DB_USER), ("DB_PASSWORD", DB_PASSWORD)) if v in (None, "")]
if missing:
    logging.error(f"Missing required DB env vars: {', '.join(missing)}. Exiting.")
    sys.exit(1)

# Concurrency
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
MAX_DB_CONNECTIONS = int(os.getenv("MAX_DB_CONNECTIONS", str(MAX_WORKERS + 2)))
if MAX_DB_CONNECTIONS < MAX_WORKERS:
    logging.warning("MAX_DB_CONNECTIONS < MAX_WORKERS; increasing MAX_DB_CONNECTIONS to match MAX_WORKERS")
    MAX_DB_CONNECTIONS = MAX_WORKERS

# Maps for FD-style CSVs (e.g., B1.csv, D1.csv)
FD_LEAGUE_MAP = {
    'B1': {'name': 'Jupiler Pro League', 'area_name': 'Belgium', 'code': 'B1'},
    'D1': {'name': 'Bundesliga', 'area_name': 'Germany', 'code': 'BL1'},
    'E0': {'name': 'Premier League', 'area_name': 'England', 'code': 'PL'},
}

# Maps for country-style CSVs (e.g., AUT.csv)
COUNTRY_LEAGUE_MAP = {
    'AUT': {'name': 'Bundesliga', 'area_name': 'Austria', 'code': 'AL1'},
    'DNK': {'name': 'Superliga', 'area_name': 'Denmark', 'code': 'DSL'},
    'CHN': {'name': 'Super League', 'area_name': 'China', 'code': 'CSL'},
    'BRA': {'name': 'Serie A', 'area_name': 'Brazil', 'code': 'BSA'},
    'ARG': {'name': 'Primera Division', 'area_name': 'Argentina', 'code': 'APD'},
}

# ============ DB POOL ============
db_pool = None
def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = ThreadedConnectionPool(
            1, MAX_DB_CONNECTIONS,
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )

# ============ Helpers ============
def to_int(val, default=None):
    if val is None or val == '':
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def to_float(val, default=None):
    if val is None or val == '':
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def infer_season_year(match_date):
    if match_date.month >= 7:
        return match_date.year
    return match_date.year - 1

def parse_date_time(row, date_key='Date', time_key='Time'):
    date_str = row.get(date_key)
    time_str = row.get(time_key, '').strip() or '00:00'
    if not date_str:
        raise ValueError("Missing date field")
        
    combined = f"{date_str} {time_str}".strip()
    
    # Try a list of common formats
    formats = [
        '%d/%m/%y %H:%M',
        '%d/%m/%Y %H:%M',
        '%d/%m/%y',
        '%d/%m/%Y',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
    ]
    
    dt = None
    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(combined, fmt)
            break
        except Exception:
            continue
            
    if dt is None and HAS_DATEUTIL:
        try:
            dt = date_parser.parse(combined)
        except Exception:
            dt = None
            
    if dt is None:
        raise ValueError(f"Unrecognized date format: {combined}")
        
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    else:
        dt = dt.astimezone(pytz.UTC)
        
    return dt

def get_winner(ftr):
    if ftr == 'H':
        return 'HOME_TEAM'
    elif ftr == 'A':
        return 'AWAY_TEAM'
    elif ftr == 'D':
        return 'DRAW'
    return None

# ============ DB UTILS ============

def _manual_id_insert(cur, table_name, pk_col, cols, values):
    """Handles the manual ID assignment fallback using PostgreSQL sequences (nextval)."""
    # 1. Get next available ID from the sequence
    try:
        cur.execute(f"SELECT nextval('{table_name}_id_seq')")
        new_id = cur.fetchone()[0]
    except Exception as e:
        # Fallback to MAX+1 if sequence fails (less ideal for concurrency, but functional)
        logging.warning(f"Sequence '{table_name}_id_seq' failed. Falling back to MAX+1. Error: {type(e).__name__} - {e}")
        cur.connection.rollback() 
        cur.execute(f"SELECT COALESCE(MAX({pk_col}), 0) + 1 FROM {table_name}")
        new_id = cur.fetchone()[0]

    # 2. Prepend the new ID to the values and insert
    new_cols = f"{pk_col}, {', '.join(cols)}"
    new_values = tuple([new_id] + list(values))
    
    placeholders = ', '.join(['%s'] * len(new_values))
    
    cur.execute(
        f"INSERT INTO {table_name} ({new_cols}) VALUES ({placeholders}) RETURNING {pk_col}",
        new_values
    )
    return cur.fetchone()[0]


def get_or_create_area(cur, name, code=None, flag=None):
    # 1. Lookup
    if name:
        cur.execute("SELECT area_id FROM areas WHERE name = %s", (name,))
        row = cur.fetchone()
        if row: return row[0]
    if code:
        cur.execute("SELECT area_id FROM areas WHERE code = %s", (code,))
        row = cur.fetchone()
        if row: return row[0]
        
    insert_name = name or code or 'Unknown Area'
    
    cols = ["name", "code", "flag"]
    values = (insert_name, code, flag)
    
    try:
        # 2. Attempt standard insert
        cur.execute(
            f"""
            INSERT INTO areas ({', '.join(cols)})
            VALUES (%s, %s, %s)
            RETr:UNING area_id
            """,
            values
        )
        area_id = cur.fetchone()[0]
    except IntegrityError as e:
        err_str = str(e).lower()
        
        cur.connection.rollback() # Mandatory rollback
        
        if 'null value in column "area_id"' in err_str or 'violates not-null constraint' in err_str:
            area_id = _manual_id_insert(cur, "areas", "area_id", cols, values)
        # Handle unique constraint if a race condition happened (e.g., another thread inserted it)
        elif 'unique constraint' in err_str:
             if name:
                cur.execute("SELECT area_id FROM areas WHERE name = %s", (name,))
                row = cur.fetchone()
                if row: return row[0]
             raise e
        else:
            raise e
            
    # Commit the area creation immediately so dependents (Competition, Team) can use its ID
    cur.connection.commit()
    return area_id


def get_or_create_competition(cur, area_id, name, code, type_='domestic_league', emblem=None):
    # 1. Lookup
    cur.execute("SELECT competition_id FROM competitions WHERE code = %s", (code,))
    row = cur.fetchone()
    if row: return row[0]
    
    cols = ["area_id", "name", "code", "type", "emblem", "last_updated"]
    values = (area_id, name, code, type_, emblem, datetime.datetime.now())
    
    try:
        # 2. Attempt standard insert
        cur.execute(
            f"""
            INSERT INTO competitions ({', '.join(cols)})
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING competition_id
            """,
            values
        )
        competition_id = cur.fetchone()[0]
    except IntegrityError as e:
        err_str = str(e).lower()
        
        cur.connection.rollback() # Mandatory rollback
        
        if 'null value in column "competition_id"' in err_str or 'violates not-null constraint' in err_str:
            competition_id = _manual_id_insert(cur, "competitions", "competition_id", cols, values)
        # Handle unique constraint if a race condition happened
        elif 'unique constraint' in err_str:
            # Re-select the row that was just inserted by the other thread
            cur.execute("SELECT competition_id FROM competitions WHERE code = %s", (code,))
            row = cur.fetchone()
            if row: 
                cur.connection.commit() # Commit the successful re-lookup
                return row[0]
            raise e
        else:
            raise e
            
    # Commit the competition creation immediately
    cur.connection.commit()
    return competition_id


def get_or_create_team(cur, name, area_id, short_name=None, tla=None, crest=None):
    if name is None: name = 'Unknown Team'
        
    # 1. Lookup (Assumes unique on name + area_id)
    cur.execute("SELECT team_id FROM teams WHERE name = %s AND area_id = %s", (name, area_id))
    row = cur.fetchone()
    if row: return row[0]

    cols = ["area_id", "name", "short_name", "tla", "crest", "last_updated"]
    values = (area_id, name, short_name, tla, crest, datetime.datetime.now())
    
    try:
        # 2. Attempt standard insert
        cur.execute(
            f"""
            INSERT INTO teams ({', '.join(cols)})
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING team_id
            """,
            values
        )
        team_id = cur.fetchone()[0]
    except IntegrityError as e:
        err_str = str(e).lower()

        cur.connection.rollback() # Mandatory rollback
        
        if 'null value in column "team_id"' in err_str or 'violates not-null constraint' in err_str:
            team_id = _manual_id_insert(cur, "teams", "team_id", cols, values)
        # Handle unique constraint if a race condition happened
        elif 'unique constraint' in err_str:
            # Re-select the row that was just inserted by the other thread
            cur.execute("SELECT team_id FROM teams WHERE name = %s AND area_id = %s", (name, area_id))
            row = cur.fetchone()
            if row: 
                cur.connection.commit() # Commit the successful re-lookup
                return row[0]
            raise e
        else:
            raise e
            
    cur.connection.commit()
    return team_id


def get_or_create_person(cur, name, first_name=None, last_name=None, date_of_birth=None, nationality=None, position=None):
    if not name: raise ValueError("Person name required")
        
    # 1. Lookup (Assumes unique on name + position)
    cur.execute(
        "SELECT person_id FROM persons WHERE name = %s AND position IS NOT DISTINCT FROM %s",
        (name, position)
    )
    row = cur.fetchone()
    if row: return row[0]

    cols = ["name", "first_name", "last_name", "date_of_birth", "nationality", "position", "last_updated"]
    values = (name, first_name, last_name, date_of_birth, nationality, position, datetime.datetime.now())
    
    try:
        # 2. Attempt standard insert
        cur.execute(
            f"""
            INSERT INTO persons ({', '.join(cols)})
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING person_id
            """,
            values
        )
        person_id = cur.fetchone()[0]
    except IntegrityError as e:
        err_str = str(e).lower()

        cur.connection.rollback() # Mandatory rollback
        
        if 'null value in column "person_id"' in err_str or 'violates not-null constraint' in err_str:
            person_id = _manual_id_insert(cur, "persons", "person_id", cols, values)
        # Handle unique constraint if a race condition happened
        elif 'unique constraint' in err_str:
            cur.execute(
                "SELECT person_id FROM persons WHERE name = %s AND position IS NOT DISTINCT FROM %s",
                (name, position)
            )
            row = cur.fetchone()
            if row: 
                cur.connection.commit() # Commit the successful re-lookup
                return row[0]
            raise e
        else:
            raise e

    cur.connection.commit()
    return person_id


def get_or_insert_match(cur, competition_id, season_year, utc_date, home_team_id, away_team_id,
                        score_fulltime_home, score_fulltime_away, score_halftime_home=None, score_halftime_away=None,
                        status='FINISHED', matchday=None, stage='REGULAR_SEASON', group_name=None,
                        score_winner=None, score_duration='REGULAR', venue=None, attendance=None,
                        source=None):
    
    # 1. Check for existing match (Source-specific duplicate check)
    # Note: Source column is intentionally excluded from the query to match schema constraints.
    cur.execute(
        """
        SELECT match_id FROM matches
        WHERE competition_id = %s AND season_year = %s AND utc_date = %s
          AND home_team_id = %s AND away_team_id = %s
        """,
        (competition_id, season_year, utc_date, home_team_id, away_team_id)
    )
    row = cur.fetchone()
    if row: return row[0], True

    # 2. Prepare values/cols for INSERT
    # Note: Source column is intentionally excluded from the insert to match schema constraints.
    cols = [
        "competition_id", "season_year", "utc_date", "status", "matchday", "stage", "group_name",
        "home_team_id", "away_team_id", "score_winner", "score_duration",
        "score_fulltime_home", "score_fulltime_away",
        "score_halftime_home", "score_halftime_away",
        "venue", "attendance", "last_updated", "details_populated"
    ]
    values = (
        competition_id, season_year, utc_date, status, matchday, stage, group_name,
        home_team_id, away_team_id, score_winner, score_duration,
        score_fulltime_home, score_fulltime_away,
        score_halftime_home, score_halftime_away,
        venue, attendance, datetime.datetime.now(), False
    )
    placeholders = ', '.join(['%s'] * len(cols))
    
    # 3. Attempt standard insert
    try:
        cur.execute(
            f"""
            INSERT INTO matches ({', '.join(cols)})
            VALUES ({placeholders})
            RETURNING match_id
            """,
            values
        )
        return cur.fetchone()[0], False
    except IntegrityError as e:
        err_str = str(e).lower()

        cur.connection.rollback() # Mandatory rollback
        
        if 'null value in column "match_id"' in err_str or 'violates not-null constraint' in err_str:
            match_id = _manual_id_insert(cur, "matches", "match_id", cols, values)
            return match_id, False
        # Handle unique constraint if a race condition happened
        elif 'unique constraint' in err_str:
            # Re-check existence as a race condition may have been the cause
            cur.execute(
                """
                SELECT match_id FROM matches
                WHERE competition_id = %s AND season_year = %s AND utc_date = %s
                  AND home_team_id = %s AND away_team_id = %s
                """,
                (competition_id, season_year, utc_date, home_team_id, away_team_id)
            )
            row = cur.fetchone()
            if row: return row[0], True
            raise e
        else:
            raise e

def insert_if_not_exists_match_odds(cur, match_id, home_win, draw, away_win):
    cur.execute("SELECT match_odds_id FROM match_odds WHERE match_id = %s", (match_id,))
    if cur.fetchone(): return
    cur.execute(
        "INSERT INTO match_odds (match_id, home_win, draw, away_win) VALUES (%s, %s, %s, %s)",
        (match_id, home_win, draw, away_win)
    )

def insert_if_not_exists_match_team_stats(cur, match_id, team_id, corner_kicks=None, fouls=None, offsides=None,
                                         red_cards=None, shots=None, shots_off_goal=None, shots_on_goal=None,
                                         yellow_cards=None):
    cur.execute("SELECT match_team_stats_id FROM match_team_stats WHERE match_id = %s AND team_id = %s", (match_id, team_id))
    if cur.fetchone(): return
    cur.execute(
        """
        INSERT INTO match_team_stats (match_id, team_id, corner_kicks, fouls, offsides,
                                      red_cards, shots, shots_off_goal, shots_on_goal,
                                      yellow_cards)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (match_id, team_id, corner_kicks, fouls, offsides,
         red_cards, shots, shots_off_goal, shots_on_goal,
         yellow_cards)
    )

def insert_if_not_exists_match_referee(cur, match_id, person_id, type_='REFEREE'):
    cur.execute("SELECT match_referee_id FROM match_referees WHERE match_id = %s AND person_id = %s", (match_id, person_id))
    if cur.fetchone(): return
    cur.execute("INSERT INTO match_referees (match_id, person_id, type) VALUES (%s, %s, %s)", (match_id, person_id, type_))

# ============ PROCESS ROW ============
def process_row(row, filename, source_type, conn):
    cur = conn.cursor()
    try:
        # Get league details
        if source_type == 'FD':
            div = row.get('Div')
            if div not in FD_LEAGUE_MAP:
                logging.warning(f"Unknown FD div {div} in {filename}, skipping row.")
                return
            league = FD_LEAGUE_MAP[div].copy()
        else:
            file_base = os.path.splitext(filename)[0].upper()
            if file_base not in COUNTRY_LEAGUE_MAP:
                logging.warning(f"Unknown country file {filename}, skipping row.")
                return
            league = COUNTRY_LEAGUE_MAP[file_base].copy()
            league_name = row.get('League')
            if league_name:
                league['name'] = league_name

        area_name = league.get('area_name')
        comp_name = league.get('name')
        comp_code = league.get('code')

        # Get/create area and competition (These functions commit immediately if an INSERT happens)
        area_id = get_or_create_area(cur, area_name)
        competition_id = get_or_create_competition(cur, area_id, comp_name, comp_code)

        # Parse date/time
        utc_date = parse_date_time(row)

        # Season year
        if 'Season' in row and row.get('Season'):
            season_year = int(str(row['Season']).split('/')[0])
        else:
            season_year = infer_season_year(utc_date)

        # Teams
        home_name = row.get('HomeTeam') or row.get('Home') or row.get('Home team') or row.get('HomeTeamName')
        away_name = row.get('AwayTeam') or row.get('Away') or row.get('Away team') or row.get('AwayTeamName')
        if not home_name or not home_name.strip() or not away_name or not away_name.strip():
            logging.warning(f"Skipping row with missing or empty teams in {filename}")
            return
            
        home_team_id = get_or_create_team(cur, home_name.strip(), area_id)
        away_team_id = get_or_create_team(cur, away_name.strip(), area_id)

        # Scores
        if source_type == 'FD':
            ft_home = to_int(row.get('FTHG'), 0)
            ft_away = to_int(row.get('FTAG'), 0)
            ht_home = to_int(row.get('HTHG'), None)
            ht_away = to_int(row.get('HTAG'), None)
            winner = get_winner(row.get('FTR'))
        else:
            ft_home = to_int(row.get('HG'), 0)
            ft_away = to_int(row.get('AG'), 0)
            ht_home = None
            ht_away = None
            winner = get_winner(row.get('Res'))

        # Venue, attendance, referee (if available)
        venue = None
        attendance = to_int(row.get('Attendance')) if 'Attendance' in row else None
        referee_name = row.get('Referee') or row.get('Referee name')
        referee_id = None
        if referee_name:
            referee_id = get_or_create_person(cur, referee_name, position='REFEREE')

        # Get or insert match
        match_id, exists = get_or_insert_match(
            cur, competition_id, season_year, utc_date, home_team_id, away_team_id,
            ft_home, ft_away, ht_home, ht_away,
            score_winner=winner, venue=venue, attendance=attendance,
            source=source_type # source_type is only used for logging/logic, not database insertion here
        )
        
        if exists:
            logging.info(f"Existing match found, skipping match-insert sub-inserts for: {home_name or 'Unknown'} vs {away_name or 'Unknown'} on {utc_date.isoformat()} (Source: {source_type})")
        
        # Insert related data only if match was new
        if not exists:
            # Insert referee if exists and not already
            if referee_id:
                insert_if_not_exists_match_referee(cur, match_id, referee_id)
                
            # Odds
            home_odds = row.get('AvgCH') or row.get('AvgH') or row.get('PSCH') or row.get('AvgHome')
            draw_odds = row.get('AvgCD') or row.get('AvgD') or row.get('PSCD') or row.get('AvgDraw')
            away_odds = row.get('AvgCA') or row.get('AvgA') or row.get('PSCA') or row.get('AvgAway')
            if home_odds and draw_odds and away_odds:
                hv = to_float(home_odds)
                dv = to_float(draw_odds)
                av = to_float(away_odds)
                if hv is not None and dv is not None and av is not None:
                    try:
                        insert_if_not_exists_match_odds(cur, match_id, hv, dv, av)
                    except Exception as e:
                        logging.warning(f"Could not insert odds for match {match_id}: {e}")
                        
            # Team stats (only FD)
            if source_type == 'FD':
                # Home stats
                home_shots = to_int(row.get('HS'), 0)
                home_shots_on = to_int(row.get('HST'), 0)
                home_shots_off = None
                if home_shots is not None and home_shots_on is not None:
                    home_shots_off = max(0, home_shots - home_shots_on)
                    
                insert_if_not_exists_match_team_stats(
                    cur, match_id, home_team_id,
                    corner_kicks=to_int(row.get('HC'), None),
                    fouls=to_int(row.get('HF'), None),
                    offsides=to_int(row.get('HO'), None),
                    red_cards=to_int(row.get('HR'), None),
                    shots=home_shots,
                    shots_off_goal=home_shots_off,
                    shots_on_goal=home_shots_on,
                    yellow_cards=to_int(row.get('HY'), None)
                )
                # Away stats
                away_shots = to_int(row.get('AS'), 0)
                away_shots_on = to_int(row.get('AST'), 0)
                away_shots_off = None
                if away_shots is not None and away_shots_on is not None:
                    away_shots_off = max(0, away_shots - away_shots_on)
                    
                insert_if_not_exists_match_team_stats(
                    cur, match_id, away_team_id,
                    corner_kicks=to_int(row.get('AC'), None),
                    fouls=to_int(row.get('AF'), None),
                    offsides=to_int(row.get('AO'), None),
                    red_cards=to_int(row.get('AR'), None),
                    shots=away_shots,
                    shots_off_goal=away_shots_off,
                    shots_on_goal=away_shots_on,
                    yellow_cards=to_int(row.get('AY'), None)
                )

        logging.info(f"Processed match: {home_name or 'Unknown'} {ft_home}-{ft_away} {away_name or 'Unknown'} ({utc_date.isoformat()}) (Source: {source_type})")
        
    except Exception as e:
        # Catch and rollback all remaining errors (including SSL issues)
        try:
            conn.rollback()
        except Exception:
            pass
            
        log_keys = ('HomeTeam', 'AwayTeam', 'Date') if source_type == 'FD' else ('Home', 'Away', 'Date')
        row_snip = {k: row.get(k) for k in log_keys}
        logging.error(f"Error processing row in {filename}: {e}; row snippet: {str(row_snip)}")
        
    finally:
        try:
            cur.close()
        except Exception:
            pass

# ============ WRAPPER for thread tasks ============
def _process_row_with_conn(row, filename, source_type):
    conn = db_pool.getconn()
    try:
        process_row(row, filename, source_type, conn)
    finally:
        db_pool.putconn(conn)

# ============ MAIN ============
def main(folder_path='dataset'):
    init_db_pool()
    try:
        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv') and 'Supabase Snippet' not in f]
    except Exception as e:
        logging.error(f"Could not list folder {folder_path}: {e}")
        return
        
    try:
        for filename in csv_files:
            source_type = 'FD' if re.match(r'^[A-Z]\d(\s*\(1\))?\.csv$', filename, flags=re.IGNORECASE) else 'COUNTRY'
            logging.info(f"Processing {filename} as {source_type} type.")
            file_path = os.path.join(folder_path, filename)
            
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = []
                        for row in reader:
                            futures.append(executor.submit(_process_row_with_conn, row, filename, source_type))
                            
                        for fut in as_completed(futures):
                            # Ensure any result/exception from the thread is handled
                            fut.result()
                            
            except Exception as e:
                logging.error(f"Failed processing file {filename}: {e}")
                
    finally:
        try:
            if db_pool:
                db_pool.closeall()
        except Exception as e:
            logging.warning(f"Error closing DB pool: {e}")
            
    logging.info("All files processed. DB pool closed.")

if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else 'dataset'
    main(folder)