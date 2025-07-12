import os
import time
import uuid
import math
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv
import requests
from db import SessionLocal, Company, Session, or_, User, JobRun, JobRunCompany
import threading
import traceback
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import logging

from google_auth import get_credentials
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO  # You can change this to DEBUG for more verbosity
)
logger = logging.getLogger(__name__)
# Configuration
load_dotenv()
API_KEY = os.getenv("GOOGLE_API_KEY")
LARGE_RADIUS_METERS = int(os.getenv("LARGE_RADIUS_METERS", "50000"))
MEDIUM_RADIUS_METERS = int(os.getenv("MEDIUM_RADIUS_METERS", "30000"))
SMALL_RADIUS_METERS = int(os.getenv("SMALL_RADIUS_METERS", "10000"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "2.0"))
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE")
if not API_KEY:
    raise RuntimeError("Set the GOOGLE_API_KEY environment variable first.")
if not GOOGLE_CREDS_FILE:
    raise RuntimeError("Set the GOOGLE_CREDS_FILE environment variable first.")

# Locations data
with open('states.json', 'r', encoding='utf-8') as file:
    LOCATIONS = json.load(file)

# search func with Places API (New)
def search_places(api_key, keyword, latitude, longitude, page_token=None, rad:int=LARGE_RADIUS_METERS):
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.id,places.location,nextPageToken"
    }
    data = {
        "textQuery": keyword,
        "locationBias": {
            "circle": {
                "center": {
                    "latitude": latitude,
                    "longitude": longitude
                },
                "radius": rad
            }
        }
    }
    if page_token:
        data["pageToken"] = page_token
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code} - {response.text}"

def get_place_details(api_key, place_id):
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "displayName,formattedAddress,internationalPhoneNumber,websiteUri,rating,location"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code} - {response.text}"

def _collect_one_location(
    db: Session,
    keyword: str,
    lat: float,
    lng: float,
    state: Optional[str]=None,
    job_run_id: Optional[int]=None,
    city_type: Optional[str]=None,
):
    existing = {row[0] for row in db.query(Company.place_id).yield_per(500)}
    seen: set[str] = set()
    grid_points = [(lat, lng)]

    for grid_lat, grid_lng in grid_points:
        page_token = None
        while True:
            response = search_places(
                API_KEY, keyword,
                grid_lat,
                grid_lng,
                page_token,
                LARGE_RADIUS_METERS if city_type == "large" else MEDIUM_RADIUS_METERS if city_type == "medium" else SMALL_RADIUS_METERS if city_type == "small" else LARGE_RADIUS_METERS
                )
            if isinstance(response, str):
                logger.error(f"Error searching ({grid_lat}, {grid_lng}): {response}")
                break
            
            for place in response.get("places", []):
                pid = place["id"]
                if pid in seen or pid in existing:
                    continue
                
                details = get_place_details(API_KEY, pid)
                if isinstance(details, str):
                    logger.error(f"Error getting details place_id {pid}: {details}")
                    continue
                
                company_data = {
                    "name": place.get("displayName", {}).get("text"),
                    "address": place.get("formattedAddress"),
                    "phone": details.get("internationalPhoneNumber"),
                    "website": details.get("websiteUri"),
                    "rating": details.get("rating"),
                    "lat": place.get("location", {}).get("latitude"),
                    "lng": place.get("location", {}).get("longitude"),
                    "keyword": keyword,
                    "state": state,
                }
                
                company = db.query(Company).filter_by(place_id=pid).first()
                now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
                if company:
                    updated_fields = []
                    for idx, field in enumerate(["phone", "website", "rating"], start=1):
                        if getattr(company, field) != company_data[field]:
                            setattr(company, field, company_data[field])
                            updated_fields.append(idx)
                    if updated_fields:
                        company.updated_at = json.dumps([updated_fields, now_iso])
                        db.add(company)
                else:
                    company = Company(
                        place_id=pid,
                        fetched_at=now_iso,
                        updated_at=None,
                        **company_data
                    )
                    db.add(company)
                    db.commit()
                
                if job_run_id:
                    existing_link = db.query(JobRunCompany).filter_by(job_run_id=job_run_id, company_id=company.id).first()
                    if not existing_link:
                        link = JobRunCompany(job_run_id=job_run_id, company_id=company.id)
                        db.add(link)
                        db.commit()
                
                seen.add(pid)
                try:
                    db.commit()
                except Exception as e:
                    logger.error(f"Error saving data for place_id {pid}: {str(e)}")
                    db.rollback()
            
            page_token = response.get("nextPageToken")
            if not page_token:
                break
            time.sleep(REQUEST_DELAY)

def geocode_city(city_name, state_code):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": f"{city_name}, {state_code}, USA",
        "key": API_KEY
    }
    resp = requests.get(url, params=params)
    if resp.status_code == 200:
        data = resp.json()
        if data["status"] == "OK":
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
    return None, None

def collect_companies(
    keyword: str,
    states: Optional[str] = None,
    task_id: Optional[str] = None,
    city_type: Optional[str] = None,
    city_name: Optional[str] = None,
    job_run_id: Optional[int] = None,
    db: Optional[Session] = None
):
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        if city_type == "manual" and city_name and states:
            lat, lng = geocode_city(city_name, states)
            if lat is None or lng is None:
                log_status(task_id, f"Could not geocode city '{city_name}' in state '{states}'.")
                return
            log_status(task_id, f"Collecting for {city_name}, {states} (manual)")
            _collect_one_location(
                db,
                keyword,
                lat,
                lng,
                states,
                job_run_id,
                city_type=None
            )
            return

        if states is None or states == "ALL":
            locations = LOCATIONS.items()
        else:
            state_data = LOCATIONS.get(states)
            if not state_data:
                log_status(task_id, f"State '{states}' not found in LOCATIONS or has no cities.")
                return
            locations = [(states, state_data)]

        for state_code, state_data in locations:
            types_to_process = [city_type] if city_type and city_type != "all" else ['large', 'medium', 'small']
            for current_type in types_to_process:
                cities = state_data.get(current_type, [])
                for city_data in cities:
                    if city_name and city_data['city'].lower() != city_name.lower():
                        continue
                    log_status(task_id, f"Collecting for {city_data['city']}, {state_code} ({current_type})")
                    _collect_one_location(
                        db,
                        keyword,
                        city_data['lat'],
                        city_data['lng'],
                        state_code,
                        job_run_id,
                        city_type=current_type
                    )
    except Exception as e:
        raise
    finally:
        if close_db:
            db.close()

class CollectorTask:
    def __init__(self, keyword: str, states: Optional[str]=None):
        self.id = str(uuid.uuid4())
        self.keyword = keyword
        self.states = states
        self.status = "in progress"


def log_status(task_id: str, message: str):
    logger.info(f"task_id {task_id}, {message}")
    log_file = f"logs\\{task_id}.log"
    with open(log_file, "a", encoding="utf-8") as lf:
        lf.write(message + "\n")

def run_collector_in_thread(keyword: str, state: Optional[str]=None, city_type: Optional[str] = None, city_name: Optional[str] = None, user_id: Optional[str] = None):
    task = CollectorTask(keyword, state)
    log_status(task.id, f"Task {task.id} started at {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    def target():
        start_time = time.time()
        db = SessionLocal()
        try:
            user = db.query(User).filter_by(user_id=user_id).first() if user_id else None
            if not user:
                raise ValueError(f"User with user_id={user_id} not found in database.")
            job_run = JobRun(
                user_email_id=user.id,
                params=json.dumps({"keyword": keyword, "state": state, "city_type": city_type, "city_name": city_name}),
                started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                finished_at=None
            )
            db.add(job_run)
            db.commit()
            collect_companies(
                keyword=keyword,
                states=state,
                task_id=task.id,
                city_type=city_type,
                city_name=city_name,
                job_run_id=job_run.id,
                db=db
            )
            job_run.finished_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            db.commit()
            task.status = "done"
        except Exception as e:
            tb = traceback.format_exc()
            log_status(task.id, f"Error occured: {str(e)}\n{tb}")
            task.status = f"failed: {str(e)}"
        finally:
            elapsed = time.time() - start_time
            log_status(task.id, f"Task {task.id} finished with status: {task.status} in {elapsed:.2f} seconds")
            db.close()
    thread = threading.Thread(target=target)
    thread.start()
    thread.join()
    return task.id

def create_google_sheet(
        spreadsheet_id: str = None,
        task_state: bool = False,  # If True, overwrite existing spreadsheet, False - Append to existing
        user_email: str = None,
        keyword: str | None = None,
        state: str | None = None,
        city_type: Optional[str] = None,
        city_name: Optional[str] = None,
        job_run_id: Optional[int] = None,
) -> str:
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)

    if not spreadsheet_id:
        raise ValueError("spreadsheet_id must be provided to write to an existing Google Sheet.")

    db = SessionLocal()
    try:
        query = db.query(Company)
        if keyword:
            query = query.filter(Company.keyword == keyword)
        if state and state != "ALL":
            query = query.filter(Company.state == state)
        if city_name:
            query = query.filter(Company.address.like(f"%{city_name}%"))
        if city_type and city_type != "all":
            if state and state != "ALL":
                cities_in_state = LOCATIONS.get(state, {})
                cities_of_type = cities_in_state.get(city_type, [])
                city_names = [city['city'] for city in cities_of_type]
                if city_names:
                    query = query.filter(or_(*[Company.address.like(f"%{name}%") for name in city_names]))
            else:
                all_cities_of_type = []
                for state_data in LOCATIONS.values():
                    all_cities_of_type.extend(city['city'] for city in state_data.get(city_type, []))
                if all_cities_of_type:
                    query = query.filter(or_(*[Company.address.like(f"%{name}%") for name in all_cities_of_type]))
        companies = query.all()
    finally:
        db.close()

    # Prepare data for Google Sheets
    headers = ["Place Id", "Name", "Address", "Phone", "Website", "Rating", "Lat", "Lng", "Keyword", "State", "Fetched At", "Updated At"]
    values = [headers]

    for company in companies:
        values.append([
            company.place_id,
            company.name,
            company.address,
            company.phone or "",
            company.website or "",
            company.rating or "",
            company.lat,
            company.lng,
            company.keyword,
            company.state,
            company.fetched_at,
            company.updated_at or ""
        ])

    if task_state:
        # Overwrite: clear and write from A1
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range="A1:Z10000"
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
    else:
        # Append: get current number of rows and append after them
        sheet = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="A1:Z10000"
        ).execute()
        existing_rows = sheet.get("values", [])
        start_row = len(existing_rows) + 1  
        if not existing_rows:
            append_values = values
        else:
            append_values = values[1:] 
        if append_values:
            service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"A{start_row}",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": append_values}
            ).execute()

    # Share with user
    if user_email:
        drive_service = build("drive", "v3", credentials=creds)
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body={"type": "user", "role": "writer", "emailAddress": user_email},
            fields="id"
        ).execute()

    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"


def create_sheet_for_user(username: str):
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    
    spreadsheet = {
        'properties': {
            'title': username + "'s sheet"
        }
    }
    
    spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
    spreadsheet_id = spreadsheet['spreadsheetId']
    
    return spreadsheet_id