import os
import time
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

HEADERS = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9,en-GB;q=0.8',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://www.onthebeach.co.uk',
    'referer': 'https://www.onthebeach.co.uk/holidays/cruise/search/search/?traveltype=Cruise+Only',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0',
    'x-requested-with': 'XMLHttpRequest',
    'cookie': 'datadome=2_HUpNxC0aFRlZbw7lgs8trJ7wvjP1cJaqZLGnch0R8EaoJ5ONt7GDgnH95URI6Xf4oOIdXHFJhMOYHhvQL69hu7rGCAjGg65sPEqTBklCE0TSDWiSEQs5CLGofSEAqa;'
}

def clean_text(text):
    """Removes hidden null bytes and weird characters that crash PostgreSQL"""
    if not text:
        return ""
    return str(text).replace('\x00', '')

def run_scraper():
    print("Starting Global OTB Cruise Crawl...")
    
    # Optional: Wipe the table clean before a fresh daily scrape
    # try:
    #     supabase.table('OTB Cruises').delete().neq('cruise_line', 'DO_NOT_DELETE').execute()
    # except Exception as e:
    #     print(f"Could not clear table: {e}")
    
    page = 1
    total_inserted = 0
    
    # 3. Pagination Loop
    while True:
        print(f"Fetching Page {page}...")
        
        # The payload dynamically changes the 'page=' number to get every cruise
        PAYLOAD = f"action=search%2Fresults&filters=cruiselines%2Cships%2Cregions%2Cdepartports%2Cvisitports%2Cdurations%2Ctraveltypes%2Cprices&page={page}&sort=date&order=asc&startdate=&enddate=&region=&cruiseline=&ship=&departport=&visitport=&duration=&price=1%2C102468&cruises=&traveltype=Cruise+Only"
        
        try:
            response = requests.post(API_URL, headers=HEADERS, data=PAYLOAD, timeout=15)
            
            if response.status_code == 403:
                print("HTTP 403: Blocked by Datadome/Cloudflare. We reached our limit.")
                break

            if response.status_code == 200:
                data = response.json()
                cruises = data.get('results', []) 
                
                if not cruises:
                    print(f"No more cruises found. Finished on page {page - 1}.")
                    break

                print(f"Found {len(cruises)} cruises on Page {page}. Inserting into database...")
                
                for item in cruises:
                    c_data = item.get('cruise', {})
                    cl_data = item.get('cruiseline', {})
                    s_data = item.get('ship', {})
                    p_data = item.get('prices_pp', {})
                    
                    itinerary_raw = c_data.get('itinerary', [])
                    itinerary_string = " - ".join(itinerary_raw) if isinstance(itinerary_raw, list) else str(itinerary_raw)
                    price_val = p_data.get('cheapest', '0')
                    cruise_link = c_data.get('link', '')
                    
                    # Grab images safely
                    ship_image_dict = s_data.get('image', {})
                    image_link = ship_image_dict.get('file', '') if isinstance(ship_image_dict, dict) else ''

                    cl_image_dict = cl_data.get('image', {})
                    cl_logo_link = cl_image_dict.get('file', '') if isinstance(cl_image_dict, dict) else ''

                    # Scrub all data before inserting
                    insert_data = {
                        "cruise_line": clean_text(cl_data.get('name', 'Unknown')),
                        "ship_name": clean_text(s_data.get('name', 'Unknown')),
                        "depart_port": clean_text(c_data.get('depart_port', 'Unknown')),
                        "itinerary": clean_text(itinerary_string),
                        "depart_date": clean_text(c_data.get('depart_date', '')),
                        "duration": clean_text(c_data.get('duration', '')),
                        "price": clean_text(price_val),
                        "url": clean_text(cruise_link),
                        "image_url": clean_text(image_link),
                        "cruise_line_logo": clean_text(cl_logo_link)
                    }
                    
                    # Protected Insertion
                    try:
                        supabase.table('OTB Cruises').insert(insert_data).execute()
                        total_inserted += 1
                    except Exception as db_err:
                        print(f"  -> Skipped 1 cruise on page {page} due to DB format error: {db_err}")
                
                # Move to the next page
                page += 1
                
                # Sleep for 2.5 seconds to act human and keep the server happy
                time.sleep(2.5)

            else:
                print(f"Failed to fetch data. Status Code: {response.status_code}")
                break

        except Exception as e:
            print(f"A network error occurred on page {page}: {e}")
            break

    print(f"--- SCRAPE COMPLETE ---")
    print(f"Successfully inserted {total_inserted} total cruises into Supabase!")

if __name__ == "__main__":
    run_scraper()
