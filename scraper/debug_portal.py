import asyncio, json, logging
from playwright.async_api import async_playwright
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger()

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()

        api_responses = []
        async def on_response(resp):
            if resp.status == 200 and any(x in resp.url for x in ["/search","/results","/api","/instrument"]):
                try:
                    ct = resp.headers.get("content-type","")
                    if "json" in ct:
                        body = await resp.json()
                        api_responses.append({"url": resp.url, "body": body})
                        log.info("API RESPONSE: %s", resp.url)
                        if isinstance(body, dict):
                            for k,v in body.items():
                                if isinstance(v, list):
                                    log.info("  key=%s count=%d", k, len(v))
                                elif isinstance(v, (int, str)):
                                    log.info("  key=%s val=%s", k, str(v)[:100])
                except Exception as e:
                    log.info("  JSON parse error: %s", e)
        page.on("response", on_response)

        log.info("Loading Dallas advanced search...")
        await page.goto("https://dallas.tx.publicsearch.us/search/advanced", wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(2000)

        # Select department
        try:
            await page.click("#department", timeout=5000)
            await page.wait_for_timeout(1000)
            await page.evaluate("""
                () => {
                    const opts = document.querySelectorAll('[class*="option"], li[role="option"]');
                    for (const o of opts) {
                        if (o.textContent.includes("Property") || o.textContent.includes("Real")) {
                            o.click(); return;
                        }
                    }
                    if (opts.length > 0) opts[0].click();
                }
            """)
            await page.wait_for_timeout(1000)
        except Exception as e:
            log.info("Dept select error: %s", e)

        # Fill date range - use wider range to ensure lots of results
        start = (datetime.now() - timedelta(days=14)).strftime("%m/%d/%Y")
        end = datetime.now().strftime("%m/%d/%Y")
        await page.fill("#recordedDateRange-start", start)
        await page.fill("#recordedDateRange-end", end)
        await page.wait_for_timeout(300)

        # Select Lis Pendens doc type
        await page.click("#docTypes-input")
        await page.wait_for_timeout(300)
        await page.type("#docTypes-input", "Lis Pendens", delay=50)
        await page.wait_for_timeout(2000)
        try:
            option = await page.wait_for_selector("[class*='option'], [role='option']", timeout=3000)
            if option:
                await option.click()
                await page.wait_for_timeout(500)
        except Exception as e:
            log.info("Option select error: %s", e)

        # Click search
        await page.click("button:has-text('Search')", timeout=5000)
        await page.wait_for_timeout(5000)

        log.info("=== AFTER SEARCH ===")
        log.info("API responses captured: %d", len(api_responses))

        # Save full API responses
        with open("debug/api_calls.json","w") as f:
            json.dump(api_responses, f, indent=2, default=str)

        # Find ALL buttons and links on results page
        log.info("=== ALL BUTTONS ===")
        btns = await page.query_selector_all("button, a[href], [role='button']")
        for btn in btns:
            try:
                txt = (await btn.inner_text()).strip()
                cls = await btn.get_attribute("class") or ""
                aria = await btn.get_attribute("aria-label") or ""
                disabled = await btn.get_attribute("disabled")
                if txt or aria:
                    log.info("  btn: text='%s' aria='%s' class='%s' disabled=%s", txt[:40], aria[:30], cls[:40], disabled)
            except: pass

        # Find pagination elements specifically
        log.info("=== PAGINATION SEARCH ===")
        for selector in ["[class*='pagination']", "[class*='pager']", "[class*='page']",
                         "nav", "[aria-label*='page']", "[aria-label*='Page']"]:
            els = await page.query_selector_all(selector)
            if els:
                log.info("Selector '%s': %d elements found", selector, len(els))
                for el in els[:5]:
                    txt = (await el.inner_text()).strip()
                    cls = await el.get_attribute("class") or ""
                    log.info("  text='%s' class='%s'", txt[:60], cls[:50])

        # Screenshot results page
        await page.screenshot(path="debug/results_page.png", full_page=True)
        log.info("Screenshot saved")

        # Check total count shown on page
        page_text = await page.inner_text("body")
        import re
        counts = re.findall(r'(\d+)\s*(?:result|record|document|total)', page_text, re.IGNORECASE)
        log.info("Count mentions: %s", counts[:10])

        await browser.close()

asyncio.run(main())
