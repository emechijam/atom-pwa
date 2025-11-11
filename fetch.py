# fetch.py
import os
import sys
import time
import json
import random
import threading
from datetime import datetime, timedelta
from collections import defaultdict
# --- GEMINI UPDATE: Import lru_cache and ThreadPoolExecutor ---
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor

import requests
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import extract

# Import models and Base
from models import Base, Area, Competition, Team, Standing, Match, SyncState

# Import DB utilities
from db import engine, Session, log_progress


# === CONFIG ===
BASE_URI = "https://api.football-data.org/v4"
API_KEY = os.getenv("FOOTBALL_DATA_API_KEY")

# NEW: List of competition codes available on the free tier
FREE_TIER_COMPETITIONS = {
    'WC',  # World Cup
    'CL',  # Champions League
    'EC',  # European Championship
    'PL',  # Premier League (ENG)
    'BL1', # Bundesliga (GER)
    'SA',  # Serie A (ITA)
    'PD',  # Primera Division (ESP)
    'FL1', # Ligue 1 (FRA)
    'DED', # Eredivisie (NED)
    'PPL', # Primeira Liga (POR)
    'BSA', # Brasileiro Série A (BRA)
}
HISTORICAL_YEARS_TO_BACKFILL = 10 # Go back 10 years

CACHE_TTL_HOURS = 24
# UPDATED: Poller values to be more lightweight
LIVE_POLL_INTERVAL_MIN = 5
FUTURE_DAYS = 2 # Poller only needs to look 2 days ahead
PAST_DAYS = 2   # Poller only needs to look 2 days back
STATIC_TTL_HOURS = 0

# Rate Limiting
MAX_REQUESTS_PER_MINUTE = 8  # Safe for free tier
REQUEST_INTERVAL = 60.0 / MAX_REQUESTS_PER_MINUTE

# --- GEMINI UPDATE: Concurrency control ---
MAX_WORKERS = 4  # Number of parallel threads for fetching

# Enrichment control
ENRICH_ONLY_FUTURE = False  # Keep as False

if not API_KEY:
    print("FATAL: FOOTBALL_DATA_API_KEY not set.", file=sys.stderr)
    API_KEY = "dummy_key_for_testing" # Provide a fallback for safety

HEADERS = {"X-Auth-Token": API_KEY}


# === SMART RATE LIMITER ===
class TokenBucketLimiter:
    def __init__(self, rate_per_minute: int):
        self.rate = rate_per_minute
        self.tokens = rate_per_minute
        self.last_refill = time.time()
        # --- GEMINI UPDATE: This lock makes the limiter thread-safe ---
        self.lock = threading.Lock() 

    def wait(self):
        with self.lock:
            now = time.time()
            time_passed = now - self.last_refill
            new_tokens = time_passed * (self.rate / 60.0)
            self.tokens = min(self.rate, self.tokens + new_tokens)
            self.last_refill = now

            if self.tokens < 1:
                sleep_time = (1 - self.tokens) * (60.0 / self.rate)
                log_progress(
                    "rate_limit",
                    f"Rate limited. Sleeping {sleep_time:.2f}s...",
                )
                time.sleep(sleep_time)
                self.tokens -= 1
            else:
                self.tokens -= 1


# --- GEMINI UPDATE: Create a single, shared, thread-safe limiter ---
rate_limiter = TokenBucketLimiter(MAX_REQUESTS_PER_MINUTE)


# === DB INIT ===
def init_db():
    try:
        log_progress("db", "Initializing database tables...")
        Base.metadata.create_all(engine)
        log_progress("db", "Database tables created successfully.")
    except SQLAlchemyError as e:
        log_progress("error", f"DB initialization failed: {e}")
        print(f"DB initialization failed: {e}", file=sys.stderr)


# === HELPER FUNCTIONS ===
def utc_to_gmt1(utc_str: str) -> str:
    try:
        utc = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        return (utc + timedelta(hours=1)).strftime("%H:%M:%S")
    except Exception:
        return utc_str


def date_gmt1(utc_str: str) -> str:
    try:
        utc = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        return (utc + timedelta(hours=1)).strftime("%d-%m-%Y")
    except Exception:
        return utc_str


def needs_refresh(session, model, filter_kwargs: dict) -> bool:
    if model == Match and not filter_kwargs.get('status'):
        return True

    ttl = CACHE_TTL_HOURS
    if model in [Area, Competition, Team]:
        ttl = STATIC_TTL_HOURS

    try:
        q = session.query(model).order_by(model.last_updated.desc())
        for key, value in filter_kwargs.items():
            q = q.filter(getattr(model, key) == value)

        latest_record = q.first()
        if not latest_record:
            return True

        # Special logic for standings (less frequent checks)
        if (
            model == Standing
            and latest_record.last_updated.hour > datetime.utcnow().hour
        ):
            return False

        if model == Match:
            return True # Always allow match refreshes

        return (datetime.utcnow() - latest_record.last_updated) > timedelta(
            hours=ttl
        )
    except Exception as e:
        log_progress(
            "warning", f"Error checking refresh for {model.__name__}: {e}"
        )
        return True


def fetch_api(path: str, params: dict = None, retries: int = 3) -> dict:
    url = f"{BASE_URI}/{path}"
    for attempt in range(retries):
        # --- GEMINI UPDATE: Use the shared, thread-safe rate limiter ---
        rate_limiter.wait()
        log_progress("fetch", f"Requesting: {path} (Attempt {attempt + 1})")
        try:
            response = requests.get(
                url, headers=HEADERS, params=params, timeout=30
            )
            if response.status_code == 429:
                wait = 2**attempt + random.uniform(0, 1)
                log_progress(
                    "rate_limit",
                    f"429 Too Many Requests. Backing off {wait:.2f}s...",
                )
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            log_progress("error", f"HTTP Error: {e}")
            if (
                response.status_code in (500, 502, 503, 504)
                and attempt < retries - 1
            ):
                time.sleep(2**attempt)
                continue
            return {}
        except requests.exceptions.RequestException as e:
            log_progress("error", f"Request Error: {e}")
            if attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            return {}
    return {}


# === DATA FETCHERS ===
def fetch_areas():
    if not needs_refresh(Session(), Area, {}):
        log_progress("skip", "Areas up to date.")
        return
    data = fetch_api("areas")
    areas = data.get("areas", [])
    with Session() as session:
        for a in areas:
            try:
                area_id = int(a["id"])
                existing = session.get(Area, area_id)
                if not existing:
                    existing = Area(id=area_id)

                existing.name = a["name"]
                existing.code = a.get("code")
                existing.flag = a["flag"]
                existing.parent_area_id = a.get("parentAreaId")
                existing.last_updated = datetime.utcnow()
                session.merge(existing)
            except (ValueError, TypeError) as e:
                log_progress("error", f"Invalid Area ID {a.get('id')}: {e}")
                session.rollback()
                continue
            except Exception as e:
                log_progress("error", f"Failed to merge Area {a.get('id')}: {e}")
                session.rollback()
                continue
        try:
            session.commit()
        except SQLAlchemyError as e:
            log_progress("error", f"Failed to commit Areas: {e}")
            session.rollback()

    log_progress("sync", f"Synced {len(areas)} areas.")


def fetch_competitions():
    if not needs_refresh(Session(), Competition, {}):
        log_progress("skip", "Competitions up to date.")
        return
    data = fetch_api("competitions")
    comps = data.get("competitions", [])
    with Session() as session:
        for c in comps:
            try:
                comp_id = int(c["id"])
                existing = session.get(Competition, comp_id)
                if not existing:
                    existing = Competition(id=comp_id)

                existing.code = c["code"]
                existing.name = c["name"]
                existing.type = c["type"]
                existing.emblem = c["emblem"]
                existing.last_updated = datetime.utcnow()
                session.merge(existing)
            except (ValueError, TypeError) as e:
                log_progress("error", f"Invalid Competition ID {c.get('id')}: {e}")
                session.rollback()
                continue
            except Exception as e:
                log_progress(
                    "error", f"Failed to merge Competition {c.get('id')}: {e}"
                )
                session.rollback()
                continue
        try:
            session.commit()
        except SQLAlchemyError as e:
            log_progress("error", f"Failed to commit Competitions: {e}")
            session.rollback()

    log_progress("sync", f"Synced {len(comps)} competitions.")


def fetch_standings(competition_code: str):
    if not needs_refresh(
        Session(), Standing, {"competition_code": competition_code}
    ):
        log_progress(
            "skip", f"Standings for {competition_code} up to date."
        )
        return
    data = fetch_api(f"competitions/{competition_code}/standings")
    standings_data = data.get("standings", [])
    if not standings_data:
        log_progress("skip", f"No standings found for {competition_code}.")
        return

    season_year = data.get("season", {}).get("startDate", "N/A-").split("-")[0]

    with Session() as session:
        for s in standings_data:
            try:
                stage = s["stage"]
                st_type = s["type"]
                group = s.get("group")

                existing = (
                    session.query(Standing)
                    .filter_by(
                        competition_code=competition_code,
                        season_year=season_year,
                        stage=stage,
                        type=st_type,
                        group=group,
                    )
                    .first()
                )

                if not existing:
                    existing = Standing(
                        competition_code=competition_code,
                        season_year=season_year,
                        stage=stage,
                        type=st_type,
                        group=group,
                    )
                
                existing.table = s["table"]
                existing.last_updated = datetime.utcnow()
                session.merge(existing)
            except Exception as e:
                log_progress(
                    "error", f"Failed to merge Standing {competition_code}: {e}"
                )
                session.rollback()
                continue
        try:
            session.commit()
        except SQLAlchemyError as e:
            log_progress("error", f"Failed to commit Standings: {e}")
            session.rollback()

    log_progress("sync", f"Synced standings for {competition_code}.")


def fetch_teams(competition_code: str):
    if not needs_refresh(Session(), Team, {}):
        log_progress("skip", f"Teams for {competition_code} up to date.")
        return
    data = fetch_api(f"competitions/{competition_code}/teams")
    teams = data.get("teams", [])
    with Session() as session:
        for t in teams:
            try:
                team_id = int(t["id"])
                existing = session.get(Team, team_id)
                if not existing:
                    existing = Team(id=team_id)
                    
                existing.name = t["name"]
                existing.tla = t["tla"]
                existing.crest = t["crest"]
                existing.address = t["address"]
                existing.website = t["website"]
                existing.venue = t["venue"]
                existing.founded = t["founded"]
                existing.last_updated = datetime.utcnow()
                session.merge(existing)
            except (ValueError, TypeError) as e:
                log_progress("error", f"Invalid Team ID {t.get('id')}: {e}")
                session.rollback()
                continue
            except Exception as e:
                log_progress("error", f"Failed to merge Team {t.get('id')}: {e}")
                session.rollback()
                continue
        try:
            session.commit()
        except SQLAlchemyError as e:
            log_progress("error", f"Failed to commit Teams: {e}")
            session.rollback()

    log_progress("sync", f"Synced {len(teams)} teams for {competition_code}.")


# === MATCH FETCH & ENRICHMENT ===

def fetch_matches_smart(
    competition_code: str,
    date_from: str = None,
    date_to: str = None,
    status: str = None,
    season: int = None,
    enrich: bool = True
):
    """
    Fetches matches for a specific comp, stable and single-threaded.
    Returns the list of match IDs it processed.
    
    REFACTORED to handle multiple query types.
    """
    params = {"competitions": competition_code}
    if date_from:
        params["dateFrom"] = date_from
    if date_to:
        params["dateTo"] = date_to
    if status:
        params["status"] = status
    if season:
        params["season"] = season

    data = fetch_api("matches", params)
    matches = data.get("matches", [])
    
    if not matches:
        log_key = f"{competition_code}"
        if season: log_key += f" {season}"
        if status: log_key += f" {status}"
        if status != "LIVE": # Don't log for 'no live matches'
            log_progress("sync", f"No matches found for {log_key}.")
        return []

    log_progress(
        "sync",
        f"Processing {len(matches)} matches for {competition_code}...",
    )

    processed_match_ids = []
    for m in matches:
        try:
            if process_match_smart(m, m['status'], enrich=enrich):
                processed_match_ids.append(m['id'])
        except Exception as e:
            log_progress(
                "error",
                f"Match processing failed for {m.get('id', 'N/A')}: {e}",
            )
    
    return processed_match_ids


def process_match_smart(match_data: dict, status: str, enrich: bool = True) -> bool:
    """
    Processes a single match: enriches it and saves to DB.
    This is the core of all sync logic.
    """
    if (not match_data.get('homeTeam') or 
        not match_data.get('awayTeam') or
        not match_data.get('competition') or
        not match_data.get('utcDate')):
        log_progress("skip", f"Skipping match {match_data.get('id')} with missing core data.")
        return False
        
    try:
        match_id = int(match_data["id"])
        home_id = int(match_data["homeTeam"]["id"])
        away_id = int(match_data["awayTeam"]["id"])
    except (ValueError, TypeError) as e:
        log_progress("error", f"Invalid Match/Team ID {match_data.get('id')}: {e}")
        return False

    home_last7, away_last7, h2h, h2h_count = None, None, None, 0

    if enrich and (not ENRICH_ONLY_FUTURE or status in ["SCHEDULED", "TIMED"]):
        try:
            home_last7 = safe_team_last7(home_id)
            away_last7 = safe_team_last7(away_id)
            h2h_data = safe_head2head(home_id, away_id)
            h2h = h2h_data['matches']
            h2h_count = h2h_data['count']
        except Exception as e:
            log_progress("enrich_error", f"Failed to enrich match {match_id}: {e}")

    try:
        with Session() as session:
            existing = session.get(Match, match_id)
            
            if enrich:
                # Live poller: always overwrite with fresh data
                needs_enrich = True
            else:
                # Backfill: only set if we have no data at all
                needs_enrich = (not existing or not existing.home_last7)
            
            if not existing:
                existing = Match(id=match_id)
                existing.home_last7 = home_last7
                existing.away_last7 = away_last7
                existing.h2h = h2h
                existing.h2h_count = h2h_count
            
            if enrich:
                existing.home_last7 = home_last7
                existing.away_last7 = away_last7
                existing.h2h = h2h
                existing.h2h_count = h2h_count

            existing.league_code = match_data["competition"]["code"]
            existing.match_name = (
                f"{match_data['homeTeam'].get('shortName', '?')} vs "
                f"{match_data['awayTeam'].get('shortName', '?')}"
            )
            existing.date_gmt1 = date_gmt1(match_data["utcDate"])
            existing.time_gmt1 = utc_to_gmt1(match_data["utcDate"])
            existing.utc_date = datetime.fromisoformat(
                match_data["utcDate"].replace("Z", "+00:00")
            )
            existing.status = match_data["status"]
            existing.raw_data = match_data
            existing.last_updated = datetime.utcnow()

            session.merge(existing)
            session.commit()
        return True
    except SQLAlchemyError as e:
        log_progress("db_error", f"Failed to save match {match_id}: {e}")
        return False
    except Exception as e:
        log_progress("process_error", f"Generic error processing {match_id}: {e}")
        return False


# --- GEMINI UPDATE: Added lru_cache for efficiency ---
@lru_cache(maxsize=256)
def safe_team_last7(team_id: int, max_retries: int = 2) -> list:
    """Fetches last 7 finished matches for a team."""
    for attempt in range(max_retries):
        try:
            data = fetch_api(
                f"teams/{team_id}/matches",
                params={"status": "FINISHED", "limit": 7},
            )
            matches = data.get("matches")
            if matches is not None:
                return [format_game(g) for g in matches[-7:]]
        except Exception as e:
            log_progress(
                "enrich_warn", f"Last7 fetch failed (Attempt {attempt + 1}): {e}"
            )
            time.sleep(1)
    return []


# --- GEMINI UPDATE: Added lru_cache for efficiency ---
@lru_cache(maxsize=256)
def safe_head2head(home_id: int, away_id: int) -> dict:
    """Fetches H2H data for two teams."""
    h2h_data = fetch_api("matches", params={"h2h": f"{home_id}x{away_id}"})
    h2h = h2h_data.get("head2head", {})
    return {
        'matches': [format_game(g) for g in h2h.get("matches", [])],
        'count': h2h.get('numberOfMatches', 0),
    }


def format_game(game: dict) -> dict:
    """Formats a match object for JSON storage (Last7/H2H)."""
    home_name = game['homeTeam'].get('shortName', game['homeTeam'].get('name', '?'))
    away_name = game['awayTeam'].get('shortName', game['awayTeam'].get('name', '?'))
    score = game.get('score', {}).get('fullTime')
    
    if score and score['home'] is not None and score['away'] is not None:
        result = f"{home_name} {score['home']}-{score['away']} {away_name}"
    else:
        result = f"{home_name} vs {away_name} (No Score)"

    return {
        "date_gmt1": date_gmt1(game["utcDate"]),
        "competition": game['competition']['code'],
        "result": result,
        "match_id": game['id'],
    }


# === ON-DEMAND ENRICHMENT (for widgets.py) ===
def enrich_single_match(match_id: int):
    """
    Fetches H2H and Last7 data for one specific match.
    Called by the UI (widgets.py) for on-demand fetching.
    """
    log_progress("on_demand", f"Fetching missing data for match {match_id}...")
    try:
        with Session() as session:
            match = session.get(Match, match_id)
            if not match:
                log_progress("on_demand_error", f"Match {match_id} not found.")
                return

            home_id = int(match.raw_data["homeTeam"]["id"])
            away_id = int(match.raw_data["awayTeam"]["id"])

            # Only fetch if missing
            if not match.home_last7:
                match.home_last7 = safe_team_last7(home_id)
            if not match.away_last7:
                match.away_last7 = safe_team_last7(away_id)
            if not match.h2h:
                h2h_data = safe_head2head(home_id, away_id)
                match.h2h = h2h_data['matches']
                match.h2h_count = h2h_data['count']
            
            match.last_updated = datetime.utcnow()
            session.merge(match)
            session.commit()
            log_progress("on_demand", f"Successfully enriched match {match_id}.")
    except Exception as e:
        log_progress("on_demand_error", f"Failed to enrich {match_id}: {e}")


# --- GEMINI UPDATE: Worker function for backfill thread pool ---
def process_competition_backfill(comp, sync_state_lookup):
    """
    Worker function to process all seasons for a single competition.
    This function is designed to be run in a thread pool.
    """
    try:
        fetch_standings(comp.code) # Fetch current standings
        fetch_teams(comp.code)     # Fetch current teams
        
        log_progress("backfill", f"Checking seasons for {comp.code}...")
        comp_data = fetch_api(f"competitions/{comp.code}")
        seasons = comp_data.get("seasons", [])
        current_year = datetime.utcnow().year
        target_year = current_year - HISTORICAL_YEARS_TO_BACKFILL

        for season in seasons:
            try:
                year_str = season['startDate'].split('-')[0]
                if not year_str.isdigit():
                    continue 
                year = int(year_str)
                
                if year < target_year:
                    continue 
                
                log_progress("backfill", f"Checking {comp.code} season {year}...")
                
                season_matches = fetch_api("matches", params={
                    "competitions": comp.code,
                    "season": year
                }).get("matches", [])

                if not season_matches:
                    log_progress("backfill", f"No matches found for {comp.code} {year}.")
                    continue

                # Get the last synced match ID *for this season*
                last_synced_id = sync_state_lookup.get((comp.code, year), 0)
                
                new_matches = [
                    m for m in season_matches if m.get('id') and int(m['id']) > last_synced_id
                ]
                
                if not new_matches:
                    log_progress("backfill_skip", f"{comp.code} {year} is up to date.")
                    continue

                log_progress("backfill", f"Found {len(new_matches)} new matches for {comp.code} {year}.")
                
                new_matches.sort(key=lambda m: int(m['id']))

                for match in new_matches:
                    if process_match_smart(match, match['status'], enrich=False):
                        # This commit-per-match is intentional for resumability
                        with Session() as state_session:
                            state = state_session.query(SyncState).filter_by(
                                competition_code=comp.code,
                                season_year=year
                            ).first()
                            if not state:
                                state = SyncState(
                                    competition_code=comp.code,
                                    season_year=year
                                )
                            state.last_synced_match_id = int(match['id'])
                            state_session.merge(state)
                            state_session.commit()
                            # This lookup update is *not* thread-safe, but it only
                            # affects this thread's view, which is fine.
                            sync_state_lookup[(comp.code, year)] = int(match['id'])

            except Exception as e:
                log_progress("backfill_error", f"Failed on season {season.get('id')} for {comp.code}: {e}")
                continue # Skip to next season
    except Exception as e:
        log_progress("backfill_error", f"Failed processing competition {comp.code}: {e}")


# === HISTORICAL BACKFILL MANAGER (RUNS ONCE) ===
def backfill_manager():
    """
    The main background task for the 10-year, resumable backfill.
    Uses a ThreadPoolExecutor to parallelize work by competition.
    """
    log_progress("backfill", "Starting HISTORICAL BACKFILL manager...")
    
    sentinel_year = 2015 
    try:
        with Session() as s:
            old_match_exists = s.query(Match).filter(
                extract('year', Match.utc_date) == sentinel_year
            ).first()
            
            if old_match_exists:
                log_progress("backfill", f"Data from {sentinel_year} already exists. Historical backfill is complete. Skipping.")
                from predict import predict_for_matches
                predict_for_matches()
                return 
            else:
                log_progress("backfill", f"No data found from {sentinel_year}. Proceeding with 10-year backfill.")
    except Exception as e:
        log_progress("backfill_error", f"Error checking for old matches: {e}. Proceeding with backfill.")

    try:
        # 1. Sync static data first (sequentially)
        fetch_areas()
        fetch_competitions()

        with Session() as s:
            free_comps = (
                s.query(Competition)
                .filter(Competition.code.in_(FREE_TIER_COMPETITIONS))
                .all()
            )
            sync_states_db = s.query(SyncState).all()
            # This lookup is read-only when passed to threads, which is safe.
            sync_state_lookup = {
                (state.competition_code, state.season_year): state.last_synced_match_id
                for state in sync_states_db
            }

        # --- GEMINI UPDATE: Use ThreadPoolExecutor ---
        log_progress("backfill", f"Starting backfill pool with {MAX_WORKERS} workers...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit each competition as a separate job
            futures = {
                executor.submit(process_competition_backfill, comp, sync_state_lookup): comp.code 
                for comp in free_comps
            }
            
            for future in futures:
                comp_code = futures[future]
                try:
                    future.result()  # Wait for thread to complete
                    log_progress("backfill", f"Completed backfill for {comp_code}.")
                except Exception as e:
                    log_progress("backfill_error", f"Thread for {comp_code} failed: {e}")
        # --- END UPDATE ---

        log_progress("backfill", "HISTORICAL BACKFILL complete.")
        from predict import predict_for_matches
        predict_for_matches()

    except Exception as e:
        log_progress("backfill_error", f"Historical backfill manager failed: {e}")


# --- GEMINI UPDATE: Worker function for live poller thread pool ---
def poll_competition_live(code: str, date_from: str, date_to: str):
    """
    Worker function to poll a single competition for live/recent data.
    Designed to be run in a thread pool.
    """
    try:
        params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "competitions": code
        }
        
        data = fetch_api("matches", params)
        matches = data.get("matches", [])
        
        if not matches:
            return
        
        log_progress("live", f"Found {len(matches)} matches in window for {code}.")
        
        for m in matches:
            try:
                # Always enrich live/recent/future matches
                process_match_smart(m, m['status'], enrich=True)
            except Exception as e:
                log_progress(
                    "error",
                    f"Live poller: Match processing failed for {m.get('id', 'N/A')}: {e}",
                )
    except Exception as e:
        log_progress("live_error", f"Failed polling competition {code}: {e}")


# === LIVE POLLER (RUNS CONTINUOUSLY) ===
def live_poller():
    """
    Runs continuously to fetch live, recent, and upcoming data.
    Uses a ThreadPoolExecutor to parallelize work by competition.
    """
    
    while True:
        try:
            log_progress("live", "Polling for recent/live/upcoming matches...")
            
            date_from = (datetime.utcnow() - timedelta(days=PAST_DAYS)).strftime("%Y-%m-%d")
            date_to = (datetime.utcnow() + timedelta(days=FUTURE_DAYS)).strftime("%Y-%m-%d")

            # --- GEMINI UPDATE: Use ThreadPoolExecutor ---
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(poll_competition_live, code, date_from, date_to): code
                    for code in FREE_TIER_COMPETITIONS
                }
                
                # Wait for all polling threads to complete
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        log_progress("live_error", f"Poll thread for {futures[future]} failed: {e}")
            # --- END UPDATE ---

            # Re-run prediction on the small window of updated matches
            from predict import predict_for_matches
            predict_for_matches()

        except Exception as e:
            log_progress("error", f"Live poller error: {e}")
        
        log_progress("live", f"Poll complete. Sleeping for {LIVE_POLL_INTERVAL_MIN} min...")
        time.sleep(LIVE_POLL_INTERVAL_MIN * 60)