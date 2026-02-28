"""SQLAlchemy database models for the job tracking pipeline."""

from datetime import datetime, timezone
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, Boolean, Float,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, sessionmaker

from .config import DATABASE_URL

Base = declarative_base()


class JobListing(Base):
    """A job/internship listing discovered from any source."""
    __tablename__ = "job_listings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    linkedin_url = Column(String, unique=True, nullable=False)
    job_title = Column(String)
    company = Column(String)
    company_linkedin_url = Column(String)
    location = Column(String)
    posted_date = Column(String)
    applicant_count = Column(String)
    job_description = Column(Text)
    source = Column(String, nullable=False)  # "linkedin" or "csv"
    search_query = Column(String)

    # Pipeline metadata
    first_seen_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_seen_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    is_active = Column(Boolean, default=True)

    # Enrichment fields (produced by transform step)
    relevance_score = Column(Float)
    seniority_tag = Column(String)  # "intern", "junior", "mid", "senior"
    work_mode = Column(String)  # "remote", "hybrid", "onsite"

    def __repr__(self):
        return f"<JobListing {self.job_title} at {self.company}>"


class Application(Base):
    """Tracks your application status for a job (loaded from CSV)."""
    __tablename__ = "applications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    linkedin_url = Column(String, unique=True, nullable=False)
    job_title = Column(String)
    company = Column(String)
    status = Column(String, default="saved")  # saved, applied, interview, offer, rejected
    applied_date = Column(String)
    deadline = Column(String)
    notes = Column(Text)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return f"<Application {self.job_title} @ {self.company} [{self.status}]>"


class ScrapeRun(Base):
    """Log of each pipeline execution for auditing and change detection."""
    __tablename__ = "scrape_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, nullable=False)
    finished_at = Column(DateTime)
    jobs_found = Column(Integer, default=0)
    jobs_new = Column(Integer, default=0)
    jobs_updated = Column(Integer, default=0)
    jobs_removed = Column(Integer, default=0)
    errors = Column(Integer, default=0)
    status = Column(String, default="running")  # running, completed, failed

    def __repr__(self):
        return f"<ScrapeRun {self.started_at} — {self.status}>"


class ChangeLog(Base):
    """Records every change detected between pipeline runs."""
    __tablename__ = "change_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, nullable=False)
    linkedin_url = Column(String, nullable=False)
    change_type = Column(String, nullable=False)  # "new", "removed", "updated"
    field_changed = Column(String)  # which field changed, if "updated"
    old_value = Column(Text)
    new_value = Column(Text)
    detected_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return f"<ChangeLog {self.change_type}: {self.linkedin_url}>"


def get_engine():
    return create_engine(DATABASE_URL, echo=False)


def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def init_db():
    """Create all tables if they don't exist."""
    engine = get_engine()
    Base.metadata.create_all(engine)
