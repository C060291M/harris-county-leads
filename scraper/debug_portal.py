"""
Debug scraper - captures actual API calls made by publicsearch.us
Run once to find the real API endpoint and request format.
"""
import asyncio, json, logging
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger()

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        captured = []

        async def on_request(req):
            if any(x in req.url for x in ["api","search","instrument","record"]):
                log.info("REQUEST: %s %s", req.method, req.url)
                try:
                    body = req.post_data
                    if body: log.info("  BODY: %s", body[:300])
                except: pass

        async def on_response(resp):
            if any(x in resp.url for x in ["api","search","instrument","record"]):
                log.info("RESPONSE: %d %s", resp.status, resp.url)
                try:
                    ct = resp.headers.get("content-type","")
                    if "json" in ct:
                        body = await resp.json()
                        captured.append({"url": resp.url, "body": body})
                        log.info("  JSON keys: %s", list(body.keys()) if isinstance(body, dict) else type(body).__name__)
                        if isinstance(body, dict):
                            for k,v in body.items():
                                if isinstance(v, list) and len(v) > 0:
                                    log.info("  %s[0]: %s", k, json.dumps(v[0], default=str)[:200])
                except Exception as e:
                    log.info("  (non-JSON: %s)", e)

        page.on("request", on_request)
        page.on("response", on_response)

        log.info("Loading Dallas advanced search...")
        await page.goto("https://dallas.tx.publicsearch.us/search/advanced", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(3000)

        log.info("Page title: %s", await page.title())

        # Screenshot what the page looks like
        await page.screenshot(path="debug/publicsearch_page.png", full_page=True)
        log.info("Screenshot saved to debug/publicsearch_page.png")

        # Print all input elements
        inputs = await page.query_selector_all("input, select, button")
        log.info("Found %d form elements", len(inputs))
        for i, el in enumerate(inputs[:20]):
            tag  = await el.evaluate("e => e.tagName")
            name = await el.get_attribute("name") or ""
            pid  = await el.get_attribute("id") or ""
            ph   = await el.get_attribute("placeholder") or ""
            typ  = await el.get_attribute("type") or ""
            cls  = await el.get_attribute("class") or ""
            log.info("  [%d] %s name=%s id=%s placeholder=%s type=%s class=%s", i, tag, name, pid, ph, typ, cls[:40])

        # Try clicking date inputs and filling
        log.info("Trying to interact with date fields...")
        try:
            await page.fill("input[type='date']", "2026-05-13")
            log.info("  Filled date input")
        except: log.info("  No date input found")

        # Try to find and click search button
        try:
            btns = await page.query_selector_all("button")
            for btn in btns:
                txt = await btn.inner_text()
                log.info("  Button: '%s'", txt.strip()[:30])
        except: pass

        # Save captured API calls
        with open("debug/api_calls.json","w") as f:
            json.dump(captured, f, indent=2, default=str)
        log.info("Saved %d API responses to debug/api_calls.json", len(captured))

        await browser.close()

asyncio.run(main())
