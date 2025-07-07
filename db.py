from sqlalchemy import Column, Integer, String, create_engine, Float, UniqueConstraint, ForeignKey, or_
from sqlalchemy.orm import declarative_base, Session, sessionmaker, relationship

# DB setup
Base = declarative_base()
engine = create_engine("sqlite:///global.db", echo=False, future=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False, class_=Session)

# Email model
class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    user_id = Column(String, unique=True, index=True)
    email = Column(String)
    created_at = Column(String)
    # Relationship to JobRun (one-to-many)
    job_runs = relationship("JobRun", back_populates="user_email")

# model for companies
class Company(Base): 
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True)
    place_id = Column(String, unique=True, index=True)
    name = Column(String)
    state = Column(String)
    address = Column(String)
    phone = Column(String)
    website = Column(String)
    rating = Column(Float)
    lat = Column(Float)
    lng = Column(Float)
    keyword = Column(String)
    fetched_at = Column(String)
    updated_at = Column(String)
    __table_args__ = (UniqueConstraint("place_id", name="uix_place"),)

# model for job runs
class JobRun(Base):
    __tablename__ = "job_runs"
    id = Column(Integer, primary_key=True)
    user_email_id = Column(Integer, ForeignKey("user.id"), nullable=False)
    params = Column(String)
    started_at = Column(String)
    finished_at = Column(String)
    # Relationship back to User
    user_email = relationship("User", back_populates="job_runs")

Base.metadata.create_all(bind=engine)