# Imports
from flask import Flask, request, Response, redirect, url_for
from dotenv import load_dotenv
import os
from pymongo import MongoClient, ASCENDING
import time
import httpx
import asyncio
import json
import sys
import re
from bs4 import BeautifulSoup
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# Setup
load_dotenv()
config = json.load(open("config.json", "r"))
settings = config['queryOptions']
srpLeaderboardSettings = config['srpLeaderboardData']
MONGO_CLIENT = MongoClient(os.getenv('MONGO_CONNECTION'))
DATABASE = MONGO_CLIENT[os.getenv('MONGO_DB_NAME')]
QUEUE = DATABASE[os.getenv('MONGO_QUEUE_COLLECTION_NAME')]
PROFILES_COLLECTION = DATABASE[os.getenv('MONGO_PROFILE_COLLECTION_NAME')]
INTERNAL_URL = "http://127.0.0.1:5000" if os.getenv('FLASK_ENV') == "development" else config['domainName']
app = Flask(__name__)


# Globals
CRON_SECRET = os.getenv('CRON_SECRET')
BASE_URL = "https://hub.shutokorevivalproject.com/timing/"
PAGES_PER_LEADERBOARD = settings['pagesPerLeaderboard']
ENTRIES_PER_LEADERBOARD = srpLeaderboardSettings['entriesPerPage']


@app.route('/')
def root() -> Response:
    """
    Handles requests to the root endpoint.

    This endpoint redirects users to the /status endpoint, where documentation can be found.

    Methods:
        GET: Redirects to the /status endpoint.

    Returns:
        Response: A redirect response to the /status URL.
    """
    return redirect(url_for('status'))


# --- APIs ---
@app.route('/status', methods=['GET'])
def status() -> Response:
    """
    Handles requests to the /status endpoint.

    This endpoint can be used to check the health of the server. 
    It responds with a message indicating that the server is running.

    Methods:
        GET: Retrieves the current status of the server.

    Returns:
        Response: A JSON object containing a message and an HTTP status code 200
    """
    return {"msg": 'Status OK, server is running'}, 200


@app.route('/workflow', methods=['GET'])
async def workflow() -> Response:
    """
    """
    start_time = time.time()

    # Select earliest submited job in queue to work with
    selected_job = select_job()
    if not selected_job: return {"msg": "No job detected, going idle."}, 200

    print(selected_job, flush=True)


    # TESTING 
    # payload = {
    #     "leaderboard": "Traffic",
    #     "stage": "C1 Outer",
    # }
    # httpx.post(f'{INTERNAL_URL}/scrape?profile=Jonathan', json=payload, timeout=None)
    await orchestrator(selected_job)


    end_time = time.time()
    duration = round(end_time - start_time, 3)
    return {
        "msg": f"Completed running job on profile '{selected_job['profile']}' lasting {duration}s.",
        "jobId": str(selected_job['_id']),
        "duration": duration
    }, 200


@app.route('/scrape', methods=['POST'])
async def scrape() -> Response:
    """
    Scrapes a section of the leaderboard.
    """
    # Decode request information
    requestDetails = request.get_json()
    profile = request.args.get('profile')
    if not requestDetails or not profile: return {"msg": "Request details or profile missing!"}, 402

    # First page is synchronous to init staggered workflow
    params = {
        "leaderboard": requestDetails['leaderboard'],
        "track": 'shuto_revival_project_beta',
        "stage": requestDetails['stage'],
        "page": 0
    }
    first_page = httpx.get(BASE_URL, params=params, timeout=None)
    page_count = obtain_page_count(first_page.text)
    queue = [first_page]
    results = []

    # Make API requests and process page during downtime (staggered)
    async with httpx.AsyncClient() as client:
        for i in range(min(PAGES_PER_LEADERBOARD, page_count)):
            params['page'] = i
            res = client.get(BASE_URL, params=params, timeout=None)
            queue.append(res)

            # Process pages
            previous_page = await queue[i] if i != 0 else queue[i]
            results += process_page(previous_page.text, profile)
           
        # Last page is synchronus to finish off staggered workflow
        previous_page = await queue[-1]
        results += process_page(previous_page.text, profile)

    # Calculate KPIs of driver profile
    print(results, flush=True)
    kpis = calculate_leaderboard_stats(results)

    return {
        "msg": "Processed succefully!",
        "sourceData": results,
        "kpis": ""
    }, 200
            

# --- Util ---
def select_job():
    """
    Gets the earliest submitted job which is still in the AWAITING state.
    """
    result = QUEUE.find_one(
        {"status": "AWAITING"},
        sort=[("submissionTime", ASCENDING)]
    )
    return result


async def orchestrator(job: dict):
    """
    The orchestrator handles the proccess of kicking off multiple containers that
    each search a subsection of the SRP leaderboards.
    """
    apiData = [
        {"leaderboard": "Traffic", "stage": "C1 Outer"},
        {"leaderboard": "Traffic", "stage": "C1 Outer"},
        {"leaderboard": "Traffic", "stage": "C1 Outer"},
        {"leaderboard": "Traffic", "stage": "C1 Outer"},
        {"leaderboard": "Traffic", "stage": "C1 Outer"},
        {"leaderboard": "Traffic", "stage": "C1 Outer"}
    ]

    # Make concurrent requests and kickoff multiple Vercel containers
    async with httpx.AsyncClient() as client:
        tasks = [api_kick_off(client, data, job['profile']) for data in apiData]
        responses = await asyncio.gather(*tasks)

    print(responses, flush=True)
    return responses


async def api_kick_off(client: httpx.AsyncClient, apiData: dict, profile: str):
    """
    Kicks off a new container scraping a subsection the leaderboard.
    """
    response = await client.post(
        f"{INTERNAL_URL}/scrape?profile={profile}",
        json=apiData,
        timeout=None
    )
    return response


def obtain_page_count(html):
    """
    """
    match = re.search(r'of (\d+) entries', html)
    return int(match.group(1)) // ENTRIES_PER_LEADERBOARD


def process_page(html, profile):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', class_='table')
    rows = table.find_all('tr')[1:]  # Skip header row
    
    user_data = []
    for row in rows:
        row_class = row.get('class', [])
        is_fastest = 'text-purple' in row_class
        
        columns = row.find_all('td')
        name = columns[2].text.strip()
        
        if name == profile:
            user_data.append({
                'rank': columns[0].text.strip(),
                'date': columns[1].text.strip(),
                'name': name,
                'car': columns[3].text.strip(),
                'input': columns[4].text.strip(),
                'tyre': columns[5].text.strip(),
                'time': columns[-1].text.strip(),
                'is_fastest': is_fastest
            })
    
    return user_data


def calculate_leaderboard_stats(results):
    ...