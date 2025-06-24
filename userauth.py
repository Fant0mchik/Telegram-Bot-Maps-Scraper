import re
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, Session, sessionmaker

# DB setup
Base = declarative_base()
engine = create_engine("sqlite:///users.db", echo=False, future=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False, class_=Session)

# Email model
class UserEmail(Base):
    __tablename__ = "user_emails"
    id = Column(Integer, primary_key=True)
    user_id = Column(String, unique=True, index=True)
    email = Column(String)

Base.metadata.create_all(bind=engine)

def is_valid_email(email: str) -> bool:
    return re.match(r"[^@]+@[^@]+\.[^@]+", email) is not None

def get_user_email(user_id: str) -> str | None:
    with SessionLocal() as db:
        entry = db.query(UserEmail).filter_by(user_id=user_id).first()
        return entry.email if entry else None

def set_user_email(user_id: str, email: str):
    with SessionLocal() as db:
        entry = db.query(UserEmail).filter_by(user_id=user_id).first()
        if entry:
            entry.email = email
        else:
            entry = UserEmail(user_id=user_id, email=email)
            db.add(entry)
        db.commit()
