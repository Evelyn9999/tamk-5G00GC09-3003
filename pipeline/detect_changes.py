"""
Step 4 — DETECT CHANGES
Compare the current scrape results with what was already in the database.
Log every change to the change_log table for auditing and reporting.
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from .models import get_session, JobListing, ChangeLog, ScrapeRun

logger = logging.getLogger(__name__)

TRACKED_FIELDS = [
    "job_title", "company", "location", "posted_date",
    "applicant_count", "job_description",
]


def detect_changes(
    current_jobs: List[Dict[str, Any]],
    run_id: int,
) -> Dict[str, int]:
    """
    Compare current scrape results against the database and record changes.

    Returns counts: {"new": N, "updated": N, "removed": N}
    """
    session = get_session()
    counts = {"new": 0, "updated": 0, "removed": 0}
    now = datetime.now(timezone.utc)

    try:
        existing_map: Dict[str, JobListing] = {
            row.linkedin_url: row
            for row in session.query(JobListing).filter_by(is_active=True).all()
        }

        current_urls = set()

        for job in current_jobs:
            url = job["linkedin_url"]
            current_urls.add(url)

            if url not in existing_map:
                session.add(ChangeLog(
                    run_id=run_id,
                    linkedin_url=url,
                    change_type="new",
                    new_value=job.get("job_title") or "untitled",
                    detected_at=now,
                ))
                counts["new"] += 1
            else:
                existing = existing_map[url]
                for field in TRACKED_FIELDS:
                    old_val = getattr(existing, field, None) or ""
                    new_val = job.get(field) or ""
                    if str(old_val).strip() != str(new_val).strip() and new_val:
                        session.add(ChangeLog(
                            run_id=run_id,
                            linkedin_url=url,
                            change_type="updated",
                            field_changed=field,
                            old_value=str(old_val)[:500],
                            new_value=str(new_val)[:500],
                            detected_at=now,
                        ))
                        counts["updated"] += 1

        for url in existing_map:
            if url not in current_urls:
                session.add(ChangeLog(
                    run_id=run_id,
                    linkedin_url=url,
                    change_type="removed",
                    old_value=existing_map[url].job_title or "untitled",
                    detected_at=now,
                ))
                counts["removed"] += 1

        session.commit()
        logger.info(
            f"Changes detected — new: {counts['new']}, "
            f"updated: {counts['updated']}, removed: {counts['removed']}"
        )

    except Exception as e:
        session.rollback()
        logger.error(f"Change detection failed: {e}")
        raise
    finally:
        session.close()

    return counts


def start_run() -> int:
    """Create a new ScrapeRun record and return its id."""
    session = get_session()
    try:
        run = ScrapeRun(
            started_at=datetime.now(timezone.utc),
            status="running",
        )
        session.add(run)
        session.commit()
        run_id = run.id
        logger.info(f"Pipeline run #{run_id} started")
        return run_id
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def finish_run(
    run_id: int,
    counts: Dict[str, int],
    errors: int = 0,
    status: str = "completed",
) -> None:
    """Update the ScrapeRun record with final stats."""
    session = get_session()
    try:
        run = session.query(ScrapeRun).get(run_id)
        if run:
            run.finished_at = datetime.now(timezone.utc)
            run.jobs_found = counts.get("total", 0)
            run.jobs_new = counts.get("new", 0)
            run.jobs_updated = counts.get("updated", 0)
            run.jobs_removed = counts.get("removed", 0)
            run.errors = errors
            run.status = status
            session.commit()
            logger.info(f"Pipeline run #{run_id} finished: {status}")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
