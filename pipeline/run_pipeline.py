"""
Main entry point — orchestrates the full pipeline.

Steps:
  1. EXTRACT   — scrape LinkedIn + read CSV
  2. TRANSFORM — validate, clean, deduplicate, enrich
  3. LOAD      — write to SQLite database
  4. DETECT    — compare with previous run, log changes
  5. REPORT    — generate weekly checklist + reminders

Usage:
    py -m pipeline.run_pipeline
"""

import asyncio
import logging
import sys
from datetime import datetime

from .models import init_db
from .extract import extract_from_linkedin, extract_from_csv
from .transform import transform
from .load import load_job_listings, load_applications, mark_missing_as_inactive
from .detect_changes import detect_changes, start_run, finish_run
from .report import generate_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def run():
    """Execute the full pipeline."""
    print("=" * 60)
    print("  JOB TRACKER PIPELINE")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Initialise database tables
    init_db()
    logger.info("Database initialised")

    # Start a new pipeline run record
    run_id = start_run()
    errors = 0

    try:
        # ── STEP 1: EXTRACT ───────────────────────────────────────
        print("\n[1/5] EXTRACT — gathering data from sources...")

        linkedin_jobs = await extract_from_linkedin()
        csv_jobs = extract_from_csv()

        total_extracted = len(linkedin_jobs) + len(csv_jobs)
        print(f"       LinkedIn: {len(linkedin_jobs)} jobs")
        print(f"       CSV:      {len(csv_jobs)} saved jobs")

        # ── STEP 2: TRANSFORM ─────────────────────────────────────
        print("\n[2/5] TRANSFORM — validating, cleaning, enriching...")

        job_listings, applications = transform(linkedin_jobs, csv_jobs)

        print(f"       {len(job_listings)} job listings ready")
        print(f"       {len(applications)} application records ready")

        # ── STEP 3: DETECT CHANGES ────────────────────────────────
        print("\n[3/5] DETECT CHANGES — comparing with previous run...")

        change_counts = detect_changes(job_listings, run_id)

        print(f"       New:     {change_counts['new']}")
        print(f"       Updated: {change_counts['updated']}")
        print(f"       Removed: {change_counts['removed']}")

        # ── STEP 4: LOAD ─────────────────────────────────────────
        print("\n[4/5] LOAD — writing to database...")

        job_stats = load_job_listings(job_listings)
        app_stats = load_applications(applications)
        current_urls = [j["linkedin_url"] for j in job_listings]
        removed = mark_missing_as_inactive(current_urls)

        print(f"       Jobs:   {job_stats['inserted']} new, {job_stats['updated']} updated")
        print(f"       Apps:   {app_stats['inserted']} new, {app_stats['updated']} updated")
        print(f"       Inactive: {removed} marked")

        # ── STEP 5: REPORT ────────────────────────────────────────
        print("\n[5/5] REPORT — generating checklist & reminders...")

        report = generate_report(run_id)
        print("\n" + report)

        # Finalise run record
        finish_run(run_id, {
            "total": total_extracted,
            "new": change_counts["new"],
            "updated": change_counts["updated"],
            "removed": change_counts["removed"],
        }, errors=errors)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        errors += 1
        finish_run(run_id, {
            "total": 0, "new": 0, "updated": 0, "removed": 0,
        }, errors=errors, status="failed")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
