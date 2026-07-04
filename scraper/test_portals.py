import asyncio, httpx
from playwright.async_api import async_playwright

async def main():
    counties = ["fort-bend","brazoria","guadalupe","comal","parker","wichita","orange","hardin"]
    
    for c in counties:
        try:
            r = httpx.get(f"https://{c}.tx.publicsearch.us", timeout=8, follow_redirects=True)
            print(f"HTTP {c}: {r.status_code}")
        except Exception as e:
            print(f"HTTP {c}: FAIL")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page()
        for c in counties:
            try:
                await page.goto(f"https://{c}.tx.publicsearch.us/search/advanced", timeout=15000)
                await page.wait_for_timeout(3000)
                title = await page.title()
                print(f"PW {c}: OK title={title[:50]}")
            except Exception as e:
                print(f"PW {c}: FAIL {str(e)[:60]}")
        await browser.close()

asyncio.run(main())
