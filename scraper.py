import os
import requests
import json
from supabase import create_client, Client

# 1. Connect to Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: Supabase credentials not found in environment variables.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 2. OTB API Setup
API_URL = 'https://www.onthebeach.co.uk/holidays/cruise/search/api/'

# We use the exact headers you pulled to mimic your browser
HEADERS = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9,en-GB;q=0.8',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://www.onthebeach.co.uk',
    'referer': 'https://www.onthebeach.co.uk/holidays/cruise/search/search/?traveltype=Cruise+Only&region=Caribbean,Europe',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0',
    'x-requested-with': 'XMLHttpRequest',
    'cookie': 'datadome=2_HUpNxC0aFRlZbw7lgs8trJ7wvjP1cJaqZLGnch0R8EaoJ5ONt7GDgnH95URI6Xf4oOIdXHFJhMOYHhvQL69hu7rGCAjGg65sPEqTBklCE0TSDWiSEQs5CLGofSEAqa;'
}

# The exact payload requesting Page 1 of Europe & Caribbean cruises
PAYLOAD = 'action=search%2Fresults&filters=cruiselines%2Cships%2Cregions%2Cdepartports%2Cvisitports%2Cdurations%2Ctraveltypes%2Cprices&page=1&sort=date&order=asc&startdate=&enddate=&region=Caribbean%2CEurope&cruiseline=&ship=&departport=&visitport=&duration=&price=1%2C102468&cruises=&traveltype=Cruise+Only'

def run_scraper():
    print("Starting OTB Cruise Crawl...")
    
    try:
        response = requests.post(API_URL, headers=HEADERS, data=PAYLOAD, timeout=15)
        
        if response.status_code == 403:
            print("HTTP 403: Blocked by Datadome/Cloudflare. We will need to route via a Scraper Proxy.")
            return

        if response.status_code == 200:
            data = response.json()
            cruises = data.get('results', []) 
            
            if not cruises:
                print("No cruises found.")
                return

            print(f"Found {len(cruises)} cruises on Page 1. Preparing for Database insertion...")
            
            # Optional: Uncomment the line below to wipe the old database records clean before inserting the new daily ones!
            # supabase.table('OTB Cruises').delete().neq('cruise_line', 'DO_NOT_DELETE').execute()

            # 4. Loop through and Insert using the exact mapped JSON keys
            for item in cruises:
                
                # Extract the nested dictionaries safely
                c_data = item.get('cruise', {})
                cl_data = item.get('cruiseline', {})
                s_data = item.get('ship', {})
                p_data = item.get('prices_pp', {})
                
                # Format the itinerary array into a nice readable string separated by hyphens
                itinerary_raw = c_data.get('itinerary', [])
                itinerary_string = " - ".join(itinerary_raw) if isinstance(itinerary_raw, list) else str(itinerary_raw)

                # Format the price safely
                price_val = p_data.get('cheapest', '0')

                insert_data = {
                    "cruise_line": str(cl_data.get('name', 'Unknown')),
                    "ship_name": str(s_data.get('name', 'Unknown')),
                    "depart_port": str(c_data.get('depart_port', 'Unknown')),
                    "itinerary": itinerary_string,
                    "depart_date": str(c_data.get('depart_date', '')),
                    "duration": str(c_data.get('duration', '')),
                    "price": str(price_val)
                }
                
                # Push to Supabase
                supabase.table('OTB Cruises').insert(insert_data).execute()
                
            print("Successfully updated Supabase Database!")

        else:
            print(f"Failed to fetch data. Status Code: {response.status_code}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run_scraper()
