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
    google_sheet_id = Column(String, nullable=True)
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
    # Relationship to JobRun through JobRunCompany
    job_runs = relationship("JobRunCompany", back_populates="company")

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
    # Relationship to Company through JobRunCompany
    companies = relationship("JobRunCompany", back_populates="job_run")

# Association table for many-to-many relationship between JobRun and Company
class JobRunCompany(Base):
    __tablename__ = "job_run_company"
    id = Column(Integer, primary_key=True)
    job_run_id = Column(Integer, ForeignKey("job_runs.id"), nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    
    # Relationships
    job_run = relationship("JobRun", back_populates="companies")
    company = relationship("Company", back_populates="job_runs")
    
    # Unique constraint to prevent duplicate associations
    __table_args__ = (UniqueConstraint("job_run_id", "company_id", name="uix_job_run_company"),)

Base.metadata.create_all(bind=engine)