"""Helper script to create a LinkedIn session file for the pipeline."""

import asyncio
from linkedin_scraper import BrowserManager, wait_for_manual_login
from .config import SESSION_FILE


async def create_session():
    print("=" * 60)
    print("  LinkedIn Session Creator")
    print("=" * 60)
    print(f"\nSession will be saved to: {SESSION_FILE}\n")
    print("Steps:")
    print("  1. A browser window will open")
    print("  2. Log in to LinkedIn manually")
    print("  3. Your session will be saved automatically")
    print("=" * 60 + "\n")

    async with BrowserManager(headless=False) as browser:
        await browser.page.goto("https://www.linkedin.com/login")
        print("Please log in to LinkedIn in the browser window...")
        print("(You have 5 minutes)\n")

        try:
            await wait_for_manual_login(browser.page, timeout=300000)
        except Exception as e:
            print(f"\nLogin failed: {e}")
            return

        await browser.save_session(str(SESSION_FILE))
        print(f"\nSession saved to {SESSION_FILE}")
        print("You can now run the pipeline: py -m pipeline")


if __name__ == "__main__":
    asyncio.run(create_session())
