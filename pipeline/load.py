"""
Step 3 — LOAD
Write the transformed data into the SQLite database via SQLAlchemy.

Handles upsert logic:
  - New jobs   → INSERT
  - Existing   → UPDATE last_seen_at and any changed fields
  - Applications → upsert by linkedin_url
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from .models import get_session, JobListing, Application

logger = logging.getLogger(__name__)


def load_job_listings(jobs: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Load job listings into the database.

    Returns counts: {"inserted": N, "updated": N}
    """
    session = get_session()
    inserted = 0
    updated = 0
    now = datetime.now(timezone.utc)

    try:
        for job in jobs:
            url = job["linkedin_url"]
            existing = session.query(JobListing).filter_by(linkedin_url=url).first()

            if existing:
                _update_listing(existing, job, now)
                updated += 1
            else:
                listing = JobListing(
                    linkedin_url=url,
                    job_title=job.get("job_title"),
                    company=job.get("company"),
                    company_linkedin_url=job.get("company_linkedin_url"),
                    location=job.get("location"),
                    posted_date=job.get("posted_date"),
                    applicant_count=job.get("applicant_count"),
                    job_description=job.get("job_description"),
                    source=job.get("source", "linkedin"),
                    search_query=job.get("search_query"),
                    relevance_score=job.get("relevance_score"),
                    seniority_tag=job.get("seniority_tag"),
                    work_mode=job.get("work_mode"),
                    first_seen_at=now,
                    last_seen_at=now,
                    is_active=True,
                )
                session.add(listing)
                inserted += 1

        session.commit()
        logger.info(f"Load complete: {inserted} inserted, {updated} updated")

    except Exception as e:
        session.rollback()
        logger.error(f"Database load failed: {e}")
        raise
    finally:
        session.close()

    return {"inserted": inserted, "updated": updated}


def load_applications(apps: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Load application records from CSV into the database.

    Returns counts: {"inserted": N, "updated": N}
    """
    session = get_session()
    inserted = 0
    updated = 0
    now = datetime.now(timezone.utc)

    try:
        for app in apps:
            url = app["linkedin_url"]
            existing = session.query(Application).filter_by(linkedin_url=url).first()

            if existing:
                existing.status = app.get("status") or existing.status
                existing.applied_date = app.get("applied_date") or existing.applied_date
                existing.deadline = app.get("deadline") or existing.deadline
                existing.notes = app.get("notes") or existing.notes
                existing.updated_at = now
                updated += 1
            else:
                record = Application(
                    linkedin_url=url,
                    job_title=app.get("job_title"),
                    company=app.get("company"),
                    status=app.get("status", "saved"),
                    applied_date=app.get("applied_date"),
                    deadline=app.get("deadline"),
                    notes=app.get("notes"),
                    updated_at=now,
                )
                session.add(record)
                inserted += 1

        session.commit()
        logger.info(f"Applications loaded: {inserted} inserted, {updated} updated")

    except Exception as e:
        session.rollback()
        logger.error(f"Application load failed: {e}")
        raise
    finally:
        session.close()

    return {"inserted": inserted, "updated": updated}


def mark_missing_as_inactive(current_urls: List[str]) -> int:
    """
    Mark jobs that were NOT found in this run as inactive.
    This detects removed/expired listings.

    Returns the number of listings marked inactive.
    """
    session = get_session()
    marked = 0

    try:
        active_listings = session.query(JobListing).filter_by(is_active=True).all()

        current_set = set(current_urls)
        for listing in active_listings:
            if listing.linkedin_url not in current_set:
                listing.is_active = False
                marked += 1

        session.commit()
        if marked:
            logger.info(f"Marked {marked} listings as inactive (no longer found)")

    except Exception as e:
        session.rollback()
        logger.error(f"Failed to mark inactive listings: {e}")
    finally:
        session.close()

    return marked


def _update_listing(existing: JobListing, new_data: Dict[str, Any], now) -> None:
    """Update an existing listing with new data where values have changed."""
    existing.last_seen_at = now
    existing.is_active = True

    updatable_fields = [
        "job_title", "company", "location", "posted_date",
        "applicant_count", "job_description", "relevance_score",
        "seniority_tag", "work_mode",
    ]
    for field in updatable_fields:
        new_val = new_data.get(field)
        if new_val is not None:
            setattr(existing, field, new_val)
