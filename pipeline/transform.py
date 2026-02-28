"""
Step 2 — TRANSFORM
Validate, clean, deduplicate, and enrich the raw extracted data.

Produces two outputs:
  - cleaned job listings  (for the job_listings table)
  - application records   (for the applications table, from CSV rows)
"""

import logging
import re
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

SENIORITY_KEYWORDS = {
    "intern": ["intern", "internship", "trainee", "working student", "harjoittelija"],
    "junior": ["junior", "entry level", "entry-level", "graduate", "new grad"],
    "mid": ["mid-level", "mid level", "intermediate"],
    "senior": ["senior", "lead", "staff", "principal", "architect"],
}

RELEVANCE_KEYWORDS = [
    "python", "javascript", "typescript", "react", "node",
    "java", "c#", ".net", "full stack", "fullstack", "full-stack",
    "backend", "back-end", "frontend", "front-end",
    "software engineer", "software developer", "web developer",
    "devops", "cloud", "aws", "azure", "docker", "kubernetes",
]

WORK_MODE_PATTERNS = {
    "remote": [r"\bremote\b", r"\betä\b"],
    "hybrid": [r"\bhybrid\b"],
    "onsite": [r"\bon.?site\b", r"\bin.?office\b"],
}


def transform(
    linkedin_jobs: List[Dict[str, Any]],
    csv_jobs: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Full transformation pipeline.

    Returns:
        (job_listings, applications)
        - job_listings: cleaned + enriched jobs for the DB
        - applications: application records from CSV
    """
    logger.info("Starting transformation step")

    validated = _validate(linkedin_jobs)
    cleaned = [_clean(j) for j in validated]
    deduplicated = _deduplicate(cleaned)
    enriched = [_enrich(j) for j in deduplicated]

    applications = _build_applications(csv_jobs)

    logger.info(
        f"Transform complete: {len(enriched)} job listings, "
        f"{len(applications)} application records"
    )
    return enriched, applications


def _validate(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop rows that are clearly invalid."""
    valid = []
    for j in jobs:
        url = j.get("linkedin_url", "")
        if not url or "linkedin.com" not in url:
            logger.warning(f"Dropping job with invalid URL: {url!r}")
            continue
        if not j.get("company"):
            logger.warning(f"Dropping job with no company: {url}")
            continue
        valid.append(j)

    dropped = len(jobs) - len(valid)
    if dropped:
        logger.info(f"Validation dropped {dropped} invalid records")
    return valid


def _clean(job: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise and clean individual field values."""
    cleaned = dict(job)

    for field in ("job_title", "company", "location"):
        val = cleaned.get(field)
        if isinstance(val, str):
            val = " ".join(val.split())
            cleaned[field] = val.strip()

    url = cleaned.get("linkedin_url", "")
    cleaned["linkedin_url"] = url.split("?")[0].rstrip("/")

    desc = cleaned.get("job_description")
    if isinstance(desc, str):
        cleaned["job_description"] = desc.strip()

    return cleaned


def _deduplicate(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate listings by LinkedIn URL."""
    seen: Dict[str, int] = {}
    unique: List[Dict[str, Any]] = []

    for j in jobs:
        url = j["linkedin_url"]
        if url in seen:
            logger.debug(f"Duplicate skipped: {url}")
            continue
        seen[url] = len(unique)
        unique.append(j)

    dupes = len(jobs) - len(unique)
    if dupes:
        logger.info(f"Deduplication removed {dupes} duplicates")
    return unique


def _enrich(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add derived fields that make the data more valuable:
      - relevance_score  (0-100)
      - seniority_tag    (intern / junior / mid / senior)
      - work_mode        (remote / hybrid / onsite)
    """
    enriched = dict(job)
    text = _searchable_text(job)

    enriched["relevance_score"] = _compute_relevance(text)
    enriched["seniority_tag"] = _detect_seniority(text)
    enriched["work_mode"] = _detect_work_mode(text)

    return enriched


def _searchable_text(job: Dict[str, Any]) -> str:
    """Combine title + description + location into one lowercase string."""
    parts = [
        job.get("job_title") or "",
        job.get("job_description") or "",
        job.get("location") or "",
    ]
    return " ".join(parts).lower()


def _compute_relevance(text: str) -> float:
    """Score 0-100 based on how many relevant keywords appear."""
    if not text:
        return 0.0
    hits = sum(1 for kw in RELEVANCE_KEYWORDS if kw in text)
    score = min(100.0, (hits / len(RELEVANCE_KEYWORDS)) * 200)
    return round(score, 1)


def _detect_seniority(text: str) -> str:
    """Classify by seniority level."""
    for level, keywords in SENIORITY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return level
    return "unknown"


def _detect_work_mode(text: str) -> str:
    """Detect remote / hybrid / onsite."""
    for mode, patterns in WORK_MODE_PATTERNS.items():
        if any(re.search(p, text, re.IGNORECASE) for p in patterns):
            return mode
    return "unknown"


def _build_applications(csv_jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build application records from CSV data, with basic validation."""
    apps = []
    valid_statuses = {"saved", "applied", "interview", "offer", "rejected"}

    for row in csv_jobs:
        url = (row.get("linkedin_url") or "").strip()
        if not url:
            continue

        status = (row.get("status") or "saved").lower()
        if status not in valid_statuses:
            logger.warning(f"Unknown status '{status}' for {url}, defaulting to 'saved'")
            status = "saved"

        apps.append({
            "linkedin_url": url.split("?")[0].rstrip("/"),
            "job_title": row.get("job_title"),
            "company": row.get("company"),
            "status": status,
            "applied_date": row.get("applied_date"),
            "deadline": row.get("deadline"),
            "notes": row.get("notes"),
        })

    return apps
