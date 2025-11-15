#!/usr/bin/env python3
"""
Football-Data.org Historical Data Fetcher

This is a single, self-contained script to fetch all match data for
the free-tier competitions on football-data.org from 2022 to the present.

It uses a thread-safe, rotating API key pool to handle rate limits
and speed up downloads.

All results are saved to a single CSV file: 'fd_historical_data_2022-2025.csv'

V1.1 Change:
- Removed redundant/invalid league codes (E0, E1, D1) that were
  causing 400/404 errors. The primary codes (PL, ELC, BL1)
  already cover this data.
"""

import csv
import logging
import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple

# ============ CONFIGURATION ============

# API keys provided by the user
API_KEYS = [
    "9621862f9c22459986d6ddb4ff61296e",
    "f70a3520a46e47d8b01e92cd0c84da90",
    "b8921351f39a42ec9d9c5e83be31f69c",
    "60beb90e029f4c6cbd3c5a70db8aa828",
    "5a1b19a163124af98bf84ed03b2ef1dd",
]

# List of 13 competitions available in the TIER ONE (free) plan
# V1.1: Removed E0, E1, and D1 as they are redundant/invalid
# and caused 400/404 errors in the logs.
# PL covers E0, ELC covers E1, BL1 covers D1.
FREE_TIER_LEAGUES = [
    "PL",   # Premier League (England)
    "CL",   # UEFA Champions League
    "BL1",  # Bundesliga (Germany)
    "SA",   # Serie A (Italy)
    "FL1",  # Ligue 1 (France)
    "PPL",  # Primeira Liga (Portugal)
    "EC",   # European Championship
    "WC",   # FIFA World Cup
    "ELC",  # Championship (England)
    "SP1",  # Primera Division (Spain)
]

# Seasons to fetch (2022 to present)
# The API uses the start year for the season (e.g., 2022-23 season is 2022)
SEASONS_TO_FETCH = [2022, 2023, 2024, 2025]

# Output file name
OUTPUT_FILE_NAME = "fd_historical_data_2022-2025.csv"

# Number of parallel workers (set to the number of keys for best performance)
MAX_WORKERS = len(API_KEYS)

# API Endpoint
API_URL = "https://api.football-data.org/v4/competitions/{league_code}/matches"

# ============ LOGGING SETUP ============

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ============ KEY ROTATOR ============

class KeyRotator:
    """A thread-safe class to rotate through a list of API keys."""
    def __init__(self, keys: List[str]):
        if not keys or all(k.startswith("YOUR_") for k in keys):
            logging.critical("API_KEYS list is empty or has not been edited.")
            raise ValueError("Please edit the API_KEYS list in the script.")
        
        self.keys = keys
        self.index = 0
        self.lock = threading.Lock()
        logging.info(f"KeyRotator initialized with {len(keys)} keys.")

    def get_key(self) -> str:
        """Get the next API key in a thread-safe manner."""
        with self.lock:
            key = self.keys[self.index]
            self.index = (self.index + 1) % len(self.keys)
            return key

# ============ API FETCHER ============

def fetch_matches_for_season(
    league_code: str, 
    season: int, 
    key_rotator: KeyRotator
) -> Optional[Dict[str, Any]]:
    """
    Fetches all matches for a single league and season.
    Returns the raw JSON response dict if successful, None otherwise.
    """
    task_id = f"{league_code} / {season}"
    api_key = key_rotator.get_key()
    headers = {'X-Auth-Token': api_key}
    params = {'season': season}
    url = API_URL.format(league_code=league_code)

    logging.info(f"Requesting data for {task_id} using key ...{api_key[-4:]}")
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)

        # Check for rate limits
        if response.status_code == 429:
            logging.warning(
                f"Rate limit hit for key ...{api_key[-4:]} on {task_id}. "
                "Sleeping for 60s. Task will be retried."
            )
            time.sleep(60)
            return None  # Signal failure so it can be retried

        # Check for other errors
        response.raise_for_status()

        logging.info(f"Successfully fetched {task_id}.")
        return response.json()

    except requests.exceptions.HTTPError as e:
        if 400 <= e.response.status_code < 500:
            # 404 Not Found, 403 Forbidden, etc.
            logging.error(
                f"Client error for {task_id}: {e.response.status_code}. "
                f"Message: {e.response.text}. Skipping this task."
            )
            return {}  # Return empty dict to signal "don't retry"
        else:
            logging.error(f"Server error for {task_id}: {e}. Retrying.")
            time.sleep(10) # Wait for server issues
            return None # Signal failure
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error for {task_id}: {e}. Retrying.")
        time.sleep(10) # Wait for network issues
        return None  # Signal failure

# ============ DATA FLATTENER ============

def flatten_match_data(response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converts the nested JSON response from the API into a list
    of flat dictionaries, ready for the CSV writer.
    """
    flat_matches = []
    competition = response_json.get('competition', {})
    league_name = competition.get('name')
    league_code = competition.get('code')
    
    matches = response_json.get('matches', [])
    
    for match in matches:
        score = match.get('score', {})
        full_time = score.get('fullTime', {})
        half_time = score.get('halfTime', {})
        extra_time = score.get('extraTime', {})
        penalties = score.get('penalties', {})
        home_team = match.get('homeTeam', {})
        away_team = match.get('awayTeam', {})
        season = match.get('season', {})
        referee = (match.get('referees') or [{}])[0] # Get first referee

        row = {
            'LeagueName': league_name,
            'LeagueCode': league_code,
            'Season': season.get('startDate'),
            'Matchday': match.get('matchday'),
            'Status': match.get('status'),
            'DateTimeUTC': match.get('utcDate'),
            'HomeTeam': home_team.get('name'),
            'AwayTeam': away_team.get('name'),
            'HomeTeamID': home_team.get('id'),
            'AwayTeamID': away_team.get('id'),
            'Winner': score.get('winner'),
            'FTHG': full_time.get('home'),
            'FTAG': full_time.get('away'),
            'HTHG': half_time.get('home'),
            'HTAG': half_time.get('away'),
            'ETHG': extra_time.get('home'),
            'ETAG': extra_time.get('away'),
            'PenH': penalties.get('home'),
            'PenA': penalties.get('away'),
            'Referee': referee.get('name'),
        }
        flat_matches.append(row)
        
    return flat_matches

# ============ MAIN EXECUTION ============

def main():
    """Main function to run the data fetching and CSV writing."""
    logging.info("--- Football-Data.org Fetcher Started ---")
    
    try:
        key_rotator = KeyRotator(API_KEYS)
    except ValueError as e:
        logging.critical(f"Setup failed: {e}")
        return

    # Create a list of all tasks to run
    tasks_to_do: List[Tuple[str, int]] = [
        (league, season) 
        for league in FREE_TIER_LEAGUES 
        for season in SEASONS_TO_FETCH
    ]
    
    all_flat_matches: List[Dict[str, Any]] = []
    retry_count = 0
    
    while tasks_to_do and retry_count < 5:
        if retry_count > 0:
            logging.warning(
                f"--- Starting retry attempt {retry_count} for "
                f"{len(tasks_to_do)} failed tasks... ---"
            )
            
        failed_tasks: List[Tuple[str, int]] = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="FD-Worker") as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(
                    fetch_matches_for_season, 
                    task[0], 
                    task[1], 
                    key_rotator
                ): task
                for task in tasks_to_do
            }

            # Process results as they complete
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result_json = future.result()
                    
                    if result_json is None:
                        # API error (like 429), needs retry
                        failed_tasks.append(task)
                    elif result_json: 
                        # Success, has data
                        flat_data = flatten_match_data(result_json)
                        all_flat_matches.extend(flat_data)
                    else:
                        # Empty dict {}, client error, don't retry
                        pass 
                        
                except Exception as e:
                    logging.error(f"Error processing result for {task}: {e}")
                    failed_tasks.append(task)
        
        tasks_to_do = failed_tasks
        if tasks_to_do:
            retry_count += 1
            time.sleep(10) # Wait before next retry pass

    if tasks_to_do:
        logging.error(
            f"--- Fetching failed after {retry_count} retries. "
            f"{len(tasks_to_do)} tasks could not be completed."
        )
    else:
        logging.info("--- All fetching tasks completed successfully! ---")

    # --- Write to CSV ---
    if not all_flat_matches:
        logging.warning("No data was fetched. CSV file will not be written.")
        return

    logging.info(f"Writing {len(all_flat_matches)} matches to {OUTPUT_FILE_NAME}...")
    
    # Define CSV headers
    headers = [
        'LeagueName', 'LeagueCode', 'Season', 'Matchday', 'Status', 
        'DateTimeUTC', 'HomeTeam', 'AwayTeam', 'HomeTeamID', 'AwayTeamID',
        'Winner', 'FTHG', 'FTAG', 'HTHG', 'HTAG',
        'ETHG', 'ETAG', 'PenH', 'PenA', 'Referee'
    ]
    
    try:
        with open(OUTPUT_FILE_NAME, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_flat_matches)
        
        logging.info(f"--- Success! Data saved to {OUTPUT_FILE_NAME} ---")
        
    except Exception as e:
        logging.error(f"Failed to write CSV file: {e}")

if __name__ == "__main__":
    # Keys are hard-coded, run main directly.
    main()