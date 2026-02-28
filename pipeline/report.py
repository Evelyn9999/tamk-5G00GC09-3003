"""
Step 5 — REPORT
Generate a human-readable weekly checklist and deadline reminders.

Outputs:
  - A text report saved to data/reports/
  - Printed summary to console
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from .models import get_session, JobListing, Application, ChangeLog
from .config import REPORT_DIR

logger = logging.getLogger(__name__)


def generate_report(run_id: int) -> str:
    """
    Build a full report for the latest pipeline run.
    Saves to data/reports/ and returns the report text.
    """
    session = get_session()

    try:
        new_jobs = _get_new_jobs(session, run_id)
        removed_jobs = _get_removed_jobs(session, run_id)
        top_jobs = _get_top_jobs(session)
        applications = _get_applications(session)
        upcoming_deadlines = _get_upcoming_deadlines(session)

        lines = []
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines.append("=" * 60)
        lines.append(f"  JOB TRACKER — Pipeline Report")
        lines.append(f"  Generated: {now}")
        lines.append("=" * 60)

        # New jobs
        lines.append(f"\n--- NEW JOBS FOUND ({len(new_jobs)}) ---")
        if new_jobs:
            for j in new_jobs:
                lines.append(f"  [{j.seniority_tag or '?'}] {j.job_title} @ {j.company}")
                lines.append(f"       Location: {j.location or 'N/A'} | Mode: {j.work_mode or 'N/A'}")
                lines.append(f"       Score: {j.relevance_score or 0}/100 | {j.linkedin_url}")
        else:
            lines.append("  No new jobs this run.")

        # Removed jobs
        lines.append(f"\n--- REMOVED / EXPIRED ({len(removed_jobs)}) ---")
        if removed_jobs:
            for url in removed_jobs:
                lines.append(f"  - {url}")
        else:
            lines.append("  None removed.")

        # Top scored active jobs
        lines.append(f"\n--- TOP 10 ACTIVE JOBS BY RELEVANCE ---")
        for i, j in enumerate(top_jobs[:10], 1):
            lines.append(f"  {i}. [{j.relevance_score or 0}] {j.job_title} @ {j.company}")
            lines.append(f"      {j.location or 'N/A'} | {j.work_mode or 'N/A'} | {j.linkedin_url}")

        # Application status summary
        lines.append(f"\n--- YOUR APPLICATIONS ({len(applications)}) ---")
        status_groups: dict = {}
        for app in applications:
            status_groups.setdefault(app.status, []).append(app)
        for status in ["applied", "interview", "offer", "saved", "rejected"]:
            group = status_groups.get(status, [])
            if group:
                lines.append(f"  {status.upper()} ({len(group)}):")
                for a in group:
                    lines.append(f"    - {a.job_title or 'N/A'} @ {a.company or 'N/A'}")

        # Deadline reminders
        lines.append(f"\n--- UPCOMING DEADLINES ---")
        if upcoming_deadlines:
            for a in upcoming_deadlines:
                lines.append(f"  !! {a.deadline} — {a.job_title} @ {a.company}")
                if a.notes:
                    lines.append(f"     Note: {a.notes}")
        else:
            lines.append("  No deadlines set.")

        # Weekly checklist
        lines.append(f"\n--- WEEKLY CHECKLIST ---")
        lines.append("  [ ] Review new job listings above")
        lines.append("  [ ] Update my_saved_jobs.csv with any new applications")
        lines.append("  [ ] Follow up on 'applied' jobs with no response")
        lines.append("  [ ] Check for expired/removed listings")
        lines.append("  [ ] Prepare for upcoming deadlines")
        lines.append("")
        lines.append("=" * 60)

        report_text = "\n".join(lines)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = REPORT_DIR / f"report_{timestamp}.txt"
        report_path.write_text(report_text, encoding="utf-8")
        logger.info(f"Report saved to {report_path}")

        return report_text

    finally:
        session.close()


def _get_new_jobs(session, run_id: int) -> List[JobListing]:
    """Get jobs that were newly found in this run."""
    new_urls = [
        row.linkedin_url for row in
        session.query(ChangeLog)
        .filter_by(run_id=run_id, change_type="new")
        .all()
    ]
    if not new_urls:
        return []
    return (
        session.query(JobListing)
        .filter(JobListing.linkedin_url.in_(new_urls))
        .order_by(JobListing.relevance_score.desc())
        .all()
    )


def _get_removed_jobs(session, run_id: int) -> List[str]:
    """Get URLs of jobs removed in this run."""
    return [
        row.linkedin_url for row in
        session.query(ChangeLog)
        .filter_by(run_id=run_id, change_type="removed")
        .all()
    ]


def _get_top_jobs(session) -> List[JobListing]:
    """Get top active jobs ordered by relevance score."""
    return (
        session.query(JobListing)
        .filter_by(is_active=True)
        .order_by(JobListing.relevance_score.desc())
        .limit(10)
        .all()
    )


def _get_applications(session) -> List[Application]:
    """Get all application records."""
    return session.query(Application).order_by(Application.status).all()


def _get_upcoming_deadlines(session) -> List[Application]:
    """Get applications that have a deadline set."""
    return (
        session.query(Application)
        .filter(Application.deadline.isnot(None))
        .filter(Application.deadline != "")
        .order_by(Application.deadline)
        .all()
    )
