import requests
import csv
import os
import time
from datetime import datetime, timedelta

# --- Configuration ---
API_KEY = "5c447790790568e2c4178ef898da698e"
BASE_URL = "https://v3.football.api-sports.io/fixtures"
OUTPUT_DIR = "matches_data"

headers = {
    'x-apisports-key': API_KEY
}

# Ensure output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def get_dates():
    """Calculates strings for Yesterday, Today, and Tomorrow."""
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    return {
        "Yesterday": yesterday.strftime('%Y-%m-%d'),
        "Today": today.strftime('%Y-%m-%d'),
        "Tomorrow": tomorrow.strftime('%Y-%m-%d')
    }

def fetch_matches_by_date(label, date_str):
    """
    Fetches all fixtures for a specific date.
    Cost: 1 API Call per execution.
    """
    print(f"Fetching {label}'s matches ({date_str})...", end=" ")
    
    try:
        response = requests.get(BASE_URL, headers=headers, params={'date': date_str})
        response.raise_for_status()
        data = response.json()
        
        if data.get('errors'):
            print(f"API Error: {data['errors']}")
            return
            
        matches = data.get('response', [])
        count = len(matches)
        print(f"Success! Found {count} matches.")
        
        if count > 0:
            save_matches_to_csv(label, date_str, matches)
            
    except Exception as e:
        print(f"Failed: {e}")

def save_matches_to_csv(label, date_str, matches):
    """Parses the nested JSON response and saves flat CSV data."""
    filename = f"{label}_{date_str}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    # We flatten the nested JSON structure into columns
    rows = []
    for item in matches:
        f = item['fixture']
        l = item['league']
        t_home = item['teams']['home']
        t_away = item['teams']['away']
        g = item['goals']
        s = item['score']
        
        # Handle potential None values for venue/referee safely
        venue_name = f['venue']['name'] if f['venue'] else None
        venue_city = f['venue']['city'] if f['venue'] else None
        
        rows.append({
            'fixture_id': f['id'],
            'date': f['date'],
            'timestamp': f['timestamp'],
            'timezone': f['timezone'],
            'referee': f['referee'],
            'status_short': f['status']['short'],  # FT, NS, 1H, etc.
            'status_long': f['status']['long'],
            'elapsed_time': f['status']['elapsed'],
            
            'league_id': l['id'],
            'league_name': l['name'],
            'country': l['country'],
            'season': l['season'],
            'round': l['round'],
            
            'home_team_id': t_home['id'],
            'home_team_name': t_home['name'],
            'home_team_winner': t_home['winner'],
            'away_team_id': t_away['id'],
            'away_team_name': t_away['name'],
            'away_team_winner': t_away['winner'],
            
            'goals_home': g['home'],
            'goals_away': g['away'],
            
            'score_ht_home': s['halftime']['home'],
            'score_ht_away': s['halftime']['away'],
            'score_ft_home': s['fulltime']['home'],
            'score_ft_away': s['fulltime']['away'],
            'score_et_home': s['extratime']['home'],
            'score_et_away': s['extratime']['away'],
            'score_pen_home': s['penalty']['home'],
            'score_pen_away': s['penalty']['away'],
            
            'venue_name': venue_name,
            'venue_city': venue_city
        })

    # Define CSV Headers
    fieldnames = [
        'fixture_id', 'date', 'timestamp', 'timezone', 'referee', 'status_short', 'status_long', 'elapsed_time',
        'league_id', 'league_name', 'country', 'season', 'round',
        'home_team_id', 'home_team_name', 'home_team_winner', 
        'away_team_id', 'away_team_name', 'away_team_winner',
        'goals_home', 'goals_away',
        'score_ht_home', 'score_ht_away', 'score_ft_home', 'score_ft_away',
        'score_et_home', 'score_et_away', 'score_pen_home', 'score_pen_away',
        'venue_name', 'venue_city'
    ]

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"   -> Saved to {filepath}")

def main():
    dates = get_dates()
    print(f"--- Fetching Matches for 3 Days ---")
    print(f"Dates: {dates['Yesterday']}, {dates['Today']}, {dates['Tomorrow']}")
    print(f"Est. API Cost: 3 Calls\n")

    # Loop through the dictionary and fetch
    for label, date_str in dates.items():
        fetch_matches_by_date(label, date_str)
        # Small sleep to be gentle on the API rate limit
        time.sleep(1.5)

    print(f"\nDone! Check the '{OUTPUT_DIR}' folder.")

if __name__ == "__main__":
    main()