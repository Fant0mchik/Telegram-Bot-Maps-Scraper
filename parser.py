import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv
import googlemaps
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
RADIUS_METERS = int(os.getenv("RADIUS_METERS", "50000"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "2.0"))
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE")
if not API_KEY:
    raise RuntimeError("Set the GOOGLE_API_KEY environment variable first.")
if not GOOGLE_CREDS_FILE:
    raise RuntimeError("Set the GOOGLE_CREDS_FILE environment variable first.")

# Locations data
with open('states.json', 'r', encoding='utf-8') as file:
    LOCATIONS = json.load(file)


# Google API client
client = googlemaps.Client(key=API_KEY)

# Collector
def _collect_one_location(db: Session, keyword: str, lat: float, lng: float, state: Optional[str]=None, job_run_id: Optional[int]=None):
    existing = {row[0] for row in db.query(Company.place_id).yield_per(500)}
    seen: set[str] = set()
    response = client.places_nearby(location=(lat, lng), radius=RADIUS_METERS, keyword=keyword)

    while True:
        for place in response.get("results", []):
            pid = place["place_id"]
            if pid in seen:
                continue
            details = client.place(place_id=pid, fields=[
                "name", "formatted_address", "international_phone_number",
                "website", "rating", "geometry",
            ])
            res = details["result"]
            company_data = dict(
                name=res.get("name"),
                address=res.get("formatted_address"),
                phone=res.get("international_phone_number"),
                website=res.get("website"),
                rating=res.get("rating"),
                lat=res.get("geometry", {}).get("location", {}).get("lat"),
                lng=res.get("geometry", {}).get("location", {}).get("lng"),
                keyword=keyword,
                state=state,
            )
            company = db.query(Company).filter_by(place_id=pid).first()
            now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
            if company:
                updated_fields = []
                for idx, field in enumerate(["phone", "website", "rating"], start=1):
                    if getattr(company, field) != company_data[field]:
                        setattr(company, field, company_data[field])
                        updated_fields.append(idx)
                if updated_fields:
                    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
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
            # Додаємо зв’язок JobRunCompany
            if job_run_id:
                existing_link = db.query(JobRunCompany).filter_by(job_run_id=job_run_id, company_id=company.id).first()
                if not existing_link:
                    link = JobRunCompany(job_run_id=job_run_id, company_id=company.id)
                    db.add(link)
                    db.commit()
            seen.add(pid)
            try:
                db.commit()
            except Exception:
                db.rollback()
        token = response.get("next_page_token")
        if not token:
            break
        time.sleep(REQUEST_DELAY)
        response = client.places_nearby(page_token=token)

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
                        job_run_id
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
    service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

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
    service = build("sheets", "v4", credentials=creds)
    
    spreadsheet = {
        'properties': {
            'title': username + "'s sheet"
        }
    }
    
    spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
    spreadsheet_id = spreadsheet['spreadsheetId']
    
    return spreadsheet_id