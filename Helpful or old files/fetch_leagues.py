import requests
import csv
import sys

# --- Configuration ---
# WARNING: This is the API key you provided. 
# For security, it's better to use an environment variable.
API_KEY = "5c447790790568e2c4178ef898da698e"
API_URL = "https://v3.football.api-sports.io/leagues"
OUTPUT_FILE = "leagues_list.csv"

# Set the headers for the API request
headers = {
    'x-apisports-key': API_KEY
}

# --- Main Script ---
print("Attempting to fetch leagues from API-Football...")

try:
    # Make the GET request to the API
    response = requests.get(API_URL, headers=headers)
    
    # This will raise an error for bad status codes (4xx or 5xx)
    response.raise_for_status() 

    # Parse the JSON response
    data = response.json()

    # Check for API-level errors (e.g., bad key)
    if data.get('errors') and (isinstance(data['errors'], list) and len(data['errors']) > 0) or (isinstance(data['errors'], dict) and len(data['errors'].keys()) > 0):
        print(f"Error: The API returned an error: {data['errors']}")
        sys.exit(1) # Exit the script

    # Check if we have results
    if data.get('results', 0) > 0 and 'response' in data:
        leagues_data = data['response']
        
        print(f"Successfully fetched {len(leagues_data)} leagues.")
        
        # Open the CSV file for writing
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            # Setup the CSV writer
            writer = csv.writer(csvfile)
            
            # Write the header row
            writer.writerow(['league_id', 'league_name', 'country_name', 'league_type'])
            
            # Loop through each league and write its data
            for item in leagues_data:
                league_id = item['league']['id']
                league_name = item['league']['name']
                league_type = item['league']['type']
                
                # Handle cases where country might be missing or 'World'
                country_name = 'N/A' # Default value
                if item.get('country') and item['country'].get('name'):
                    country_name = item['country']['name']
                
                # Write the row to the CSV
                writer.writerow([league_id, league_name, country_name, league_type])
        
        print(f"Success! Data saved to {OUTPUT_FILE}")

    else:
        print("Error: No league results found in the API response.")

except requests.exceptions.HTTPError as errh:
    print(f"HTTP Error: {errh}")
    # Check for common authentication or subscription errors
    if response.status_code in [401, 403]:
        print("Error details: This may be due to an invalid API key, an expired subscription, or request limits.")
        print(f"Response text: {response.text}")
except requests.exceptions.ConnectionError as errc:
    print(f"Error Connecting: {errc}")
except requests.exceptions.Timeout as errt:
    print(f"Timeout Error: {errt}")
except requests.exceptions.RequestException as err:
    print(f"An unexpected error occurred: {err}")
except Exception as e:
    print(f"An error occurred during CSV writing or data parsing: {e}")