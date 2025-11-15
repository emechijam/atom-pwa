import requests
import csv
import sys

# --- Configuration ---
API_KEY = "5c447790790568e2c4178ef898da698e"
API_URL = "https://v3.football.api-sports.io/leagues"
OUTPUT_FILE = "leagues_full_details.csv"

# Set the headers for the API request
headers = {
    'x-apisports-key': API_KEY
}

def get_current_season(seasons_list):
    """
    Helper function to find the current season year from the list of seasons.
    If no season is marked 'current', returns the most recent year.
    """
    if not seasons_list:
        return None, None
    
    # Try to find the season marked as current=True
    for season in seasons_list:
        if season.get('current'):
            return season.get('year'), season.get('start')
            
    # Fallback: Get the last season in the list (usually the most recent)
    last_season = seasons_list[-1]
    return last_season.get('year'), last_season.get('start')

# --- Main Script ---
print("Attempting to fetch full league details from API-Football...")

try:
    # Make the GET request to the API
    response = requests.get(API_URL, headers=headers)
    
    # This will raise an error for bad status codes (4xx or 5xx)
    response.raise_for_status() 

    # Parse the JSON response
    data = response.json()

    # Check for API-level errors
    if data.get('errors') and (isinstance(data['errors'], list) and len(data['errors']) > 0) or (isinstance(data['errors'], dict) and len(data['errors'].keys()) > 0):
        print(f"Error: The API returned an error: {data['errors']}")
        sys.exit(1)

    # Check if we have results
    if data.get('results', 0) > 0 and 'response' in data:
        leagues_data = data['response']
        
        print(f"Successfully fetched {len(leagues_data)} leagues.")
        
        # Open the CSV file for writing
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            # Define comprehensive column names
            fieldnames = [
                'league_id', 
                'league_name', 
                'league_type', 
                'league_logo', 
                'country_name', 
                'country_code', 
                'country_flag',
                'current_season_year', # Useful for your next API calls
                'current_season_start'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write the header row
            writer.writeheader()
            
            # Loop through each league and parse the complex JSON structure
            for item in leagues_data:
                league = item['league']
                country = item['country']
                seasons = item['seasons']
                
                # Get current season info
                curr_year, curr_start = get_current_season(seasons)

                # Construct the row dictionary
                row = {
                    'league_id': league.get('id'),
                    'league_name': league.get('name'),
                    'league_type': league.get('type'),
                    'league_logo': league.get('logo'),
                    'country_name': country.get('name'),
                    'country_code': country.get('code'),
                    'country_flag': country.get('flag'),
                    'current_season_year': curr_year,
                    'current_season_start': curr_start
                }
                
                # Write the row to the CSV
                writer.writerow(row)
        
        print(f"Success! Full details saved to {OUTPUT_FILE}")

    else:
        print("Error: No league results found in the API response.")

except requests.exceptions.HTTPError as errh:
    print(f"HTTP Error: {errh}")
except requests.exceptions.ConnectionError as errc:
    print(f"Error Connecting: {errc}")
except requests.exceptions.Timeout as errt:
    print(f"Timeout Error: {errt}")
except requests.exceptions.RequestException as err:
    print(f"An unexpected error occurred: {err}")
except Exception as e:
    print(f"An error occurred during CSV writing or data parsing: {e}")