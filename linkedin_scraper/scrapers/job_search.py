"""
Job search scraper for LinkedIn.

Searches for jobs on LinkedIn and extracts job URLs.
"""
import logging
from typing import Optional, List
from urllib.parse import urlencode
from playwright.async_api import Page

from ..callbacks import ProgressCallback, SilentCallback
from .base import BaseScraper

logger = logging.getLogger(__name__)


class JobSearchScraper(BaseScraper):
    """
    Scraper for LinkedIn job search results.
    
    Example:
        async with BrowserManager() as browser:
            scraper = JobSearchScraper(browser.page)
            job_urls = await scraper.search(
                keywords="software engineer",
                location="San Francisco",
                limit=10
            )
    """
    
    def __init__(self, page: Page, callback: Optional[ProgressCallback] = None):
        """
        Initialize job search scraper.
        
        Args:
            page: Playwright page object
            callback: Optional progress callback
        """
        super().__init__(page, callback or SilentCallback())
    
    async def search(
        self,
        keywords: Optional[str] = None,
        location: Optional[str] = None,
        time_filter: Optional[str] = None,
        limit: int = 25
    ) -> List[str]:
        """
        Search for jobs on LinkedIn.
        
        Args:
            keywords: Job search keywords (e.g., "software engineer")
            location: Job location (e.g., "San Francisco, CA")
            time_filter: Time filter — "day", "week", or "month"
            limit: Maximum number of job URLs to return
            
        Returns:
            List of job posting URLs
        """
        logger.info(f"Starting job search: keywords='{keywords}', location='{location}', time='{time_filter}'")
        
        base_search_url = self._build_search_url(keywords, location, time_filter)
        await self.callback.on_start("JobSearch", base_search_url)

        job_urls = []
        seen_urls = set()
        page_num = 0
        jobs_per_page = 25

        while len(job_urls) < limit:
            start = page_num * jobs_per_page
            page_url = f"{base_search_url}&start={start}" if '?' in base_search_url else f"{base_search_url}?start={start}"

            logger.info(f"Loading search results page {page_num + 1} (start={start})")
            await self.navigate_and_wait(page_url)
            await self.callback.on_progress(f"Loading page {page_num + 1}", 20)

            try:
                await self.page.wait_for_selector('a[href*="/jobs/view/"]', timeout=10000)
            except:
                logger.info("No more job listings found")
                break

            await self.wait_and_focus(1)
            await self.scroll_page_to_bottom(pause_time=1, max_scrolls=5)

            new_urls = await self._extract_job_urls(limit - len(job_urls), seen_urls)
            if not new_urls:
                break

            job_urls.extend(new_urls)
            for url in new_urls:
                seen_urls.add(url)

            await self.callback.on_progress(f"Found {len(job_urls)} job URLs so far", 50)
            page_num += 1

        await self.callback.on_progress("Search complete", 100)
        await self.callback.on_complete("JobSearch", job_urls)
        
        logger.info(f"Job search complete: found {len(job_urls)} jobs across {page_num + 1} page(s)")
        return job_urls
    
    TIME_FILTER_MAP = {
        "day": "r86400",
        "week": "r604800",
        "month": "r2592000",
    }

    def _build_search_url(
        self,
        keywords: Optional[str] = None,
        location: Optional[str] = None,
        time_filter: Optional[str] = None,
    ) -> str:
        """Build LinkedIn job search URL with parameters."""
        base_url = "https://www.linkedin.com/jobs/search/"
        
        params = {}
        if keywords:
            params['keywords'] = keywords
        if location:
            params['location'] = location
        if time_filter and time_filter in self.TIME_FILTER_MAP:
            params['f_TPR'] = self.TIME_FILTER_MAP[time_filter]
        
        if params:
            return f"{base_url}?{urlencode(params)}"
        return base_url
    
    async def _extract_job_urls(self, limit: int, seen_urls: Optional[set] = None) -> List[str]:
        """
        Extract job URLs from the current search results page.
        
        Args:
            limit: Maximum number of new URLs to extract
            seen_urls: Set of already-seen URLs to skip
            
        Returns:
            List of new job posting URLs found on this page
        """
        if seen_urls is None:
            seen_urls = set()

        job_urls = []
        
        try:
            job_links = await self.page.locator('a[href*="/jobs/view/"]').all()
            
            for link in job_links:
                if len(job_urls) >= limit:
                    break
                
                try:
                    href = await link.get_attribute('href')
                    if href and '/jobs/view/' in href:
                        clean_url = href.split('?')[0] if '?' in href else href
                        
                        if not clean_url.startswith('http'):
                            clean_url = f"https://www.linkedin.com{clean_url}"
                        
                        if clean_url not in seen_urls:
                            job_urls.append(clean_url)
                            seen_urls.add(clean_url)
                except Exception as e:
                    logger.debug(f"Error extracting job URL: {e}")
                    continue
        
        except Exception as e:
            logger.warning(f"Error extracting job URLs: {e}")
        
        return job_urls
