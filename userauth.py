from datetime import datetime
import re
import time
from db import SessionLocal, User


def is_valid_email(email: str) -> bool:
    return re.match(r"[^@]+@[^@]+\.[^@]+", email) is not None

def get_user_email(user_id: str) -> str | None:
    with SessionLocal() as db:
        entry = db.query(User).filter_by(user_id=user_id).first()
        return entry.email if entry else None

def set_user_email(user_id: str, email: str, username: str):
    from parser import create_sheet_for_user
    with SessionLocal() as db:
        entry = db.query(User).filter_by(user_id=user_id).first()
        if entry:
            entry.email = email
        else:
            now_iso = datetime.now().isoformat(timespec="seconds")
            unique_sheet = create_sheet_for_user(username)
            entry = User(user_id=user_id, email=email, created_at=now_iso, google_sheet_id=unique_sheet)
            db.add(entry)
        db.commit()
