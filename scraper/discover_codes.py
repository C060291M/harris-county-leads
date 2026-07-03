import asyncio, re, os
from playwright.async_api import async_playwright

COUNTIES_TO_CHECK = {
    "fort-bend":  "https://fort-bend.tx.publicsearch.us",
    "brazoria":   "https://brazoria.tx.publicsearch.us",
    "guadalupe":  "https://guadalupe.tx.publicsearch.us",
    "comal":      "https://comal.tx.publicsearch.us",
    "parker":     "https://parker.tx.publicsearch.us",
    "wichita":    "https://wichita.tx.publicsearch.us",
    "orange":     "https://orange.tx.publicsearch.us",
    "hardin":     "https://hardin.tx.publicsearch.us",
}

SEARCH_TERMS = ["LIS","JUDG","TAX","MECHANIC","PROBATE","LIEN","NOTICE","TRUSTEE","FORECLOS","HOSPITAL","HEIRSHIP","DIVORCE","CHILD","ABSTRACT"]

async def get_codes(page, base, county):
    all_codes = {}
    for term in SEARCH_TERMS:
        try:
            await page.goto(f"{base}/search/advanced")
            await page.wait_for_timeout(2000)
            await page.click("input[placeholder='Filter Document Types']")
            await page.wait_for_timeout(1000)
            btns = await page.query_selector_all("#docTypes-listbox button")
            if btns: await btns[0].click(); await page.wait_for_timeout(1500)
            await page.fill("input[placeholder='Filter Document Types']", term)
            await page.wait_for_timeout(800)
            cbs = await page.evaluate("""
                () => Array.from(document.querySelectorAll('#docTypes-listbox input[type="checkbox"]'))
                    .map(i => ({name: i.name, id: i.id})).filter(i => i.name)
            """)
            for cb in cbs:
                if cb['name'] in all_codes: continue
                if any(x in cb['name'].upper() for x in ["RELEASE","WITHDRAWAL","DISCHARGE","CANCEL"]): continue
                clicked = await page.evaluate(f"""
                    () => {{ const l = document.querySelector('[for="{cb["id"]}"]') || document.getElementById('{cb["id"]}')?.parentElement; if(l){{l.click();return true;}} return false; }}
                """)
                if not clicked: continue
                await page.fill("input[placeholder='Filter Document Types']", "")
                await page.wait_for_timeout(200)
                await page.click("button[type='submit']")
                await page.wait_for_timeout(3000)
                url = page.url
                code_m = re.search(r'docTypes=([^&]+)', url)
                if code_m:
                    all_codes[cb['name']] = code_m.group(1)
                await page.go_back()
                await page.wait_for_timeout(800)
        except Exception as e:
            pass
    
    print(f"\n=== {county} ===")
    for name, code in all_codes.items():
        print(f"  {name}: {code}")
    return all_codes

async def main():
    target = os.getenv("COUNTY", "")
    counties = {k:v for k,v in COUNTIES_TO_CHECK.items() if not target or k == target}
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page(viewport={"width":1280,"height":800})
        for county, base in counties.items():
            await get_codes(page, base, county)
        await browser.close()

asyncio.run(main())
