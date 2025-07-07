import os
import time
import csv
import sys
import uuid
import subprocess
import argparse
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv
import googlemaps
from db import SessionLocal, Company, Session, or_
from threading import Lock
from io import StringIO
import threading
import traceback
import json
from typing import List, Dict

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from telegram import Update
from telegram.ext import ContextTypes

from userauth import get_user_email

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
def _collect_one_location(db: Session, keyword: str, lat: float, lng: float, state: Optional[str]=None):
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
    city_name: Optional[str] = None
):
    db = SessionLocal()
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
                        state_code
                    )
    except Exception as e:
        raise
    finally:
        db.close()

class CollectorTask:
    def __init__(self, keyword: str, states: Optional[str]=None):
        self.id = str(uuid.uuid4())
        self.keyword = keyword
        self.states = states
        self.status = "in progress"


def log_status(task_id: str, message: str):
    log_file = f"{task_id}.log"
    with open(log_file, "a", encoding="utf-8") as lf:
        lf.write(message + "\n")

def run_collector_in_thread(keyword: str, state: Optional[str]=None, city_type: Optional[str] = None, city_name: Optional[str] = None):
    task = CollectorTask(keyword, state)
    
    log_status(task.id, f"Task {task.id} started at {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    
    def target():
        start_time = time.time()
        try:
            collect_companies(
                keyword=task.keyword,
                states=task.states,
                task_id=task.id,
                city_type=city_type,
                city_name=city_name
            )
            task.status = "done"
        except Exception as e:
            tb = traceback.format_exc()
            log_status(task.id, f"Error occured: {str(e)}\n{tb}")
            task.status = f"failed: {str(e)}"
        finally:
            elapsed = time.time() - start_time
            log_status(task.id, f"Task {task.id} finished with status: {task.status} in {elapsed:.2f} seconds")
    
    thread = threading.Thread(target=target)
    thread.start()
    thread.join()
    return task.id

def create_google_sheet(
        title: str = "New Sheet", 
        user_email: str = None, 
        keyword: str|None = None, 
        state: str|None=None,
        city_type: Optional[str] = None,
        city_name: Optional[str] = None
) -> str:
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDS_FILE,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    
    # Create new spreadsheet
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets().create(body={"properties": {"title": title}}).execute()
    spreadsheet_id = sheet["spreadsheetId"]

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
                query = query.filter(or_(*[Company.address.like(f"%{name}%") for name in city_names]))
            else:
                all_cities_of_type = []
                for state_data in LOCATIONS.values():
                    all_cities_of_type.extend(city['city'] for city in state_data.get(city_type, []))
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

    # Write data to sheet
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="A1",
        valueInputOption="RAW",
        body={"values": values}
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

