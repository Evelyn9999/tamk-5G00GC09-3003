"""
Step 1 — EXTRACT
Gather raw data from two sources:
  - Source 1: LinkedIn job search (via Playwright scraper)
  - Source 2: CSV file of manually saved/applied jobs
"""

import asyncio
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any

from linkedin_scraper.scrapers.job_search import JobSearchScraper
from linkedin_scraper.scrapers.job import JobScraper
from linkedin_scraper.core.browser import BrowserManager
from linkedin_scraper.core.exceptions import RateLimitError, ScrapingError

from .config import (
    SESSION_FILE, CSV_FILE, SEARCHES,
    TIME_FILTER, LIMIT_PER_SEARCH, DELAY_BETWEEN_JOBS, MAX_RETRIES,
)

logger = logging.getLogger(__name__)


async def extract_from_linkedin() -> List[Dict[str, Any]]:
    """
    Extract job listings from LinkedIn using the scraper library.

    Returns a list of raw job dicts, each tagged with source="linkedin".
    Includes retry logic for individual job scrapes.
    """
    all_jobs: List[Dict[str, Any]] = []
    seen_urls: set = set()

    async with BrowserManager(headless=False) as browser:
        session_path = str(SESSION_FILE)
        if not Path(session_path).exists():
            raise FileNotFoundError(
                f"Session file not found at {session_path}. "
                "Run 'py pipeline/create_session.py' first."
            )
        await browser.load_session(session_path)
        logger.info("LinkedIn session loaded")

        search_scraper = JobSearchScraper(browser.page)
        job_scraper = JobScraper(browser.page)

        for search in SEARCHES:
            kw = search["keywords"]
            loc = search["location"]
            logger.info(f"Searching: '{kw}' in '{loc}' (past {TIME_FILTER})")

            try:
                job_urls = await search_scraper.search(
                    keywords=kw,
                    location=loc,
                    time_filter=TIME_FILTER,
                    limit=LIMIT_PER_SEARCH,
                )
            except RateLimitError:
                logger.warning(f"Rate-limited during search '{kw}'. Skipping.")
                continue
            except Exception as e:
                logger.error(f"Search failed for '{kw}': {e}")
                continue

            logger.info(f"  Found {len(job_urls)} listings")

            for url in job_urls:
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                job_data = await _scrape_job_with_retry(job_scraper, url, kw)
                if job_data:
                    all_jobs.append(job_data)

                await asyncio.sleep(DELAY_BETWEEN_JOBS)

    logger.info(f"LinkedIn extraction complete: {len(all_jobs)} jobs")
    return all_jobs


async def _scrape_job_with_retry(
    scraper: JobScraper, url: str, search_query: str
) -> Dict[str, Any] | None:
    """Scrape a single job URL with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            job = await scraper.scrape(url)
            data = job.model_dump()
            data["source"] = "linkedin"
            data["search_query"] = search_query
            logger.info(f"  Scraped: {job.job_title} at {job.company}")
            return data
        except RateLimitError:
            logger.warning(f"  Rate-limited on {url}. Waiting 60s (attempt {attempt})")
            await asyncio.sleep(60)
        except Exception as e:
            logger.warning(f"  Attempt {attempt} failed for {url}: {e}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(5 * attempt)

    logger.error(f"  All retries exhausted for {url}")
    return None


def extract_from_csv() -> List[Dict[str, Any]]:
    """
    Extract saved/applied jobs from the user's CSV file (Source 2).

    Expected CSV columns:
      linkedin_url, job_title, company, status, applied_date, deadline, notes

    Returns a list of dicts, each tagged with source="csv".
    """
    csv_path = Path(CSV_FILE)
    if not csv_path.exists():
        logger.warning(f"CSV file not found at {csv_path}. Skipping CSV source.")
        return []

    jobs: List[Dict[str, Any]] = []
    required_columns = {"linkedin_url"}

    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            if reader.fieldnames is None:
                logger.error("CSV file is empty or has no header row")
                return []

            headers = set(reader.fieldnames)
            missing = required_columns - headers
            if missing:
                logger.error(f"CSV missing required columns: {missing}")
                return []

            for row_num, row in enumerate(reader, start=2):
                url = (row.get("linkedin_url") or "").strip()
                if not url:
                    logger.warning(f"CSV row {row_num}: empty linkedin_url, skipping")
                    continue

                jobs.append({
                    "linkedin_url": url,
                    "job_title": (row.get("job_title") or "").strip() or None,
                    "company": (row.get("company") or "").strip() or None,
                    "status": (row.get("status") or "saved").strip().lower(),
                    "applied_date": (row.get("applied_date") or "").strip() or None,
                    "deadline": (row.get("deadline") or "").strip() or None,
                    "notes": (row.get("notes") or "").strip() or None,
                    "source": "csv",
                })

    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return []

    logger.info(f"CSV extraction complete: {len(jobs)} saved jobs")
    return jobs
