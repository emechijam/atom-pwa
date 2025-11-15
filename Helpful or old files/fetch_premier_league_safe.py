import requests
import csv
import os
import time
import sys

# --- Configuration ---
# API_KEY: The key provided by the user
API_KEY = "5c447790790568e2c4178ef898da698e" 
BASE_URL = "https://v3.football.api-sports.io"
LEAGUE_ID = 39       # Premier League
SEASON = 2023        # The 2023-2024 Season
OUTPUT_DIR = "premier_league_2023_data_safe"

# Headers
headers = {
    'x-apisports-key': API_KEY
}

# Ensure output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def make_request(endpoint, params=None):
    """Helper to make API requests with error handling."""
    url = f"{BASE_URL}/{endpoint}"
    try:
        print(f"  -> Fetching: {endpoint} ...", end=" ")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get('errors'):
            # Handle API-specific errors (like rate limits)
            print(f"\nAPI Error: {data['errors']}")
            return None
            
        results_count = data.get('results', 0)
        print(f"Success! ({results_count} results)")
        return data.get('response', [])
        
    except Exception as e:
        print(f"\nFailed: {e}")
        return None

def save_to_csv(filename, fieldnames, rows):
    """Helper to save list of dicts to CSV."""
    filepath = os.path.join(OUTPUT_DIR, filename)
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"     Saved {filename}")
    except Exception as e:
        print(f"     Error saving {filename}: {e}")

# --- 1. Fetch Teams & Venues (Cost: 1 Call) ---
def fetch_teams():
    print("\n[1/4] Fetching Teams & Venues (Cost: 1 Call)...")
    data = make_request('teams', {'league': LEAGUE_ID, 'season': SEASON})
    if not data: return []

    teams_rows = []
    venues_rows = []
    team_ids = []

    for item in data:
        t = item['team']
        v = item['venue']
        
        teams_rows.append({
            'team_id': t['id'],
            'name': t['name'],
            'code': t['code'],
            'founded': t['founded'],
            'national': t['national'],
            'logo': t['logo'],
            'venue_id': v['id']
        })
        
        venues_rows.append({
            'venue_id': v['id'],
            'name': v['name'],
            'address': v['address'],
            'city': v['city'],
            'capacity': v['capacity'],
            'surface': v['surface']
        })
        
        team_ids.append(t['id'])

    save_to_csv('teams.csv', ['team_id', 'name', 'code', 'founded', 'national', 'logo', 'venue_id'], teams_rows)
    save_to_csv('venues.csv', ['venue_id', 'name', 'address', 'city', 'capacity', 'surface'], venues_rows)
    
    return team_ids

# --- 2. Fetch Standings (Cost: 1 Call) ---
def fetch_standings():
    print("\n[2/4] Fetching Standings (Cost: 1 Call)...")
    data = make_request('standings', {'league': LEAGUE_ID, 'season': SEASON})
    if not data: return

    # API returns a nested structure for standings
    # response[0]['league']['standings'][0] holds the table rows
    try:
        standings_list = data[0]['league']['standings'][0]
    except (IndexError, KeyError, TypeError):
        print("     Could not parse standings structure.")
        return

    rows = []
    for rank in standings_list:
        rows.append({
            'rank': rank['rank'],
            'team_id': rank['team']['id'],
            'team_name': rank['team']['name'],
            'points': rank['points'],
            'goals_diff': rank['goalsDiff'],
            'form': rank['form'],
            'played': rank['all']['played'],
            'win': rank['all']['win'],
            'draw': rank['all']['draw'],
            'lose': rank['all']['lose'],
            'goals_for': rank['all']['goals']['for'],
            'goals_against': rank['all']['goals']['against']
        })

    save_to_csv('standings.csv', ['rank', 'team_id', 'team_name', 'points', 'goals_diff', 'form', 'played', 'win', 'draw', 'lose', 'goals_for', 'goals_against'], rows)

# --- 3. Fetch Fixtures/Matches (Cost: 1 Call) ---
def fetch_fixtures():
    print("\n[3/4] Fetching Fixtures/Schedule (Cost: 1 Call)...")
    data = make_request('fixtures', {'league': LEAGUE_ID, 'season': SEASON})
    if not data: return

    rows = []
    for item in data:
        f = item['fixture']
        l = item['league']
        t_home = item['teams']['home']
        t_away = item['teams']['away']
        g = item['goals']
        s = item['score']

        rows.append({
            'fixture_id': f['id'],
            'date': f['date'],
            'status_short': f['status']['short'],  # FT, NS, etc.
            'status_long': f['status']['long'],
            'venue_id': f['venue']['id'],
            'round': l['round'],
            'home_team_id': t_home['id'],
            'home_team_name': t_home['name'],
            'away_team_id': t_away['id'],
            'away_team_name': t_away['name'],
            'goals_home': g['home'],
            'goals_away': g['away'],
            'ht_score_home': s['halftime']['home'],
            'ht_score_away': s['halftime']['away'],
            'ft_score_home': s['fulltime']['home'],
            'ft_score_away': s['fulltime']['away']
        })

    save_to_csv('fixtures.csv', ['fixture_id', 'date', 'status_short', 'status_long', 'venue_id', 'round', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'goals_home', 'goals_away', 'ht_score_home', 'ht_score_away', 'ft_score_home', 'ft_score_away'], rows)

# --- 4. Fetch Top Scorers (Cost: 1 Call) ---
def fetch_top_scorers():
    print("\n[4/4] Fetching Top Scorers (Cost: 1 Call)...")
    data = make_request('players/topscorers', {'league': LEAGUE_ID, 'season': SEASON})
    if not data: return

    rows = []
    for item in data:
        p = item['player']
        stats = item['statistics'][0] 
        
        rows.append({
            'player_id': p['id'],
            'name': p['name'],
            'team_id': stats['team']['id'],
            'team_name': stats['team']['name'],
            'goals': stats['goals']['total'],
            'assists': stats['goals']['assists'],
            'appearances': stats['games']['appearences'],
            'minutes': stats['games']['minutes'],
            'rating': stats['games']['rating']
        })

    save_to_csv('top_scorers.csv', ['player_id', 'name', 'team_id', 'team_name', 'goals', 'assists', 'appearances', 'minutes', 'rating'], rows)

# --- Main Execution ---
def main():
    print(f"--- Starting SAFE Data Collection for Premier League {SEASON} ---")
    print("Constraint: Low API Call limit.")
    print("Plan: Fetching Teams, Standings, Fixtures, and Top Scorers.")
    print(f"Total Estimated API Calls: 4")
    print(f"Output Directory: {OUTPUT_DIR}")
    
    # 1. Teams
    fetch_teams()
    
    # 2. Standings
    fetch_standings()
    
    # 3. Fixtures
    fetch_fixtures()
    
    # 4. Stats
    fetch_top_scorers()
    
    print(f"\nDone! Essential data saved in folder: '{OUTPUT_DIR}'")
    print("Note: 'Squads' (Players list) were skipped to save ~20 API calls.")

if __name__ == "__main__":
    main()