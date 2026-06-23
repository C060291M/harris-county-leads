import asyncio, json
from playwright.async_api import async_playwright

# All countytx-web counties that are producing 0 records
COUNTIES = {
    "andrews":    "https://andrewscountytx-web.tylerhost.net/web",
    "aransas":    "https://aransascountytx-web.tylerhost.net/web",
    "bastrop":    "https://bastropcountytx-web.tylerhost.net/web",
    "bowie":      "https://bowiecountytx-web.tylerhost.net/web",
    "calhoun":    "https://calhouncountytx-web.tylerhost.net/web",
    "carson":     "https://carsoncountytx-web.tylerhost.net/web",
    "dallam":     "https://dallamcountytx-web.tylerhost.net/web",
    "eastland":   "https://eastlandcountytx-web.tylerhost.net/web",
    "erath":      "https://erathcountytx-web.tylerhost.net/web",
    "gonzales":   "https://gonzalescountytx-web.tylerhost.net/web",
    "gregg":      "https://greggcountytx-web.tylerhost.net/web",
    "guadalupe":  "https://guadalupecountytx-web.tylerhost.net/web",
    "hamilton":   "https://hamiltoncountytx-web.tylerhost.net/web",
    "hardin":     "https://hardincountytx-web.tylerhost.net/web",
    "harrison":   "https://harrisoncountytx-web.tylerhost.net/web",
    "hays":       "https://hayscountytx-web.tylerhost.net/web",
    "henderson":  "https://hendersoncountytx-web.tylerhost.net/web",
    "hill":       "https://hillcountytx-web.tylerhost.net/web",
    "hood":       "https://hoodcountytx-web.tylerhost.net/web",
    "howard":     "https://howardcountytx-web.tylerhost.net/web",
    "hunt":       "https://huntcountytx-web.tylerhost.net/web",
    "jasper":     "https://jaspercountytx-web.tylerhost.net/web",
    "karnes":     "https://karnescountytx-web.tylerhost.net/web",
    "kaufman":    "https://kaufmancountytx-web.tylerhost.net/web",
    "kimble":     "https://kimblecountytx-web.tylerhost.net/web",
    "lamar":      "https://lamarcountytx-web.tylerhost.net/web",
    "lavaca":     "https://lavacacountytx-web.tylerhost.net/web",
    "liberty":    "https://libertycountytx-web.tylerhost.net/web",
    "lubbock":    "https://lubbockcountytx-web.tylerhost.net/web",
    "mclennan":   "https://mclennancountytx-web.tylerhost.net/web",
    "orange":     "https://orangecountytx-web.tylerhost.net/web",
    "palopinto":  "https://palopintocountytx-web.tylerhost.net/web",
    "panola":     "https://panolacountytx-web.tylerhost.net/web",
    "polk":       "https://polkcountytx-web.tylerhost.net/web",
    "randall":    "https://randallcountytx-web.tylerhost.net/web",
    "somervell":  "https://somervellcountytx-web.tylerhost.net/web",
    "tomgreen":   "https://tomgreencountytx-web.tylerhost.net/web",
    "upshur":     "https://upshurcountytx-web.tylerhost.net/web",
    "vanzandt":   "https://vanzandtcountytx-web.tylerhost.net/web",
    "waller":     "https://wallercountytx-web.tylerhost.net/web",
    "wichita":    "https://wichitacountytx-web.tylerhost.net/web",
    "winkler":    "https://winklercountytx-web.tylerhost.net/web",
    "wise":       "https://wisecountytx-web.tylerhost.net/web",
    "wood":       "https://woodcountytx-web.tylerhost.net/web",
    "yoakum":     "https://yoakumcountytx-web.tylerhost.net/web",
    # tx-web variants
    "comal":      "https://comaltx-web.tylerhost.net/web",
    "hays":       "https://haystx-web.tylerhost.net/web",
    "rockwall":   "https://rockwalltx-web.tylerhost.net/web",
    "kaufman":    "https://kaufmantx-web.tylerhost.net/web",
}

async def get_docsearch(page, county, base):
    try:
        await page.goto(base + '/user/disclaimer', timeout=15000)
        await page.wait_for_timeout(1500)
        await page.evaluate("(() => { const b = document.querySelector('button'); if(b){ b.removeAttribute('disabled'); b.click(); } })()")
        await page.wait_for_timeout(1500)
        
        # Find Official Records link
        links = await page.query_selector_all('a[href*=ACTIONGROUP]')
        action_url = None
        for l in links:
            text = await l.inner_text()
            if 'Official' in text or 'Record' in text:
                href = await l.get_attribute('href')
                action_url = base + href if href.startswith('/') else href
                break
        
        if not action_url:
            # Try direct DOCSEARCH
            links2 = await page.query_selector_all('a[href*=DOCSEARCH]')
            for l in links2:
                text = await l.inner_text()
                href = await l.get_attribute('href')
                if 'Official' in text or 'Plat' not in text:
                    return href.split('/')[-1] if href else None
            return None
        
        await page.goto(action_url, timeout=15000)
        await page.wait_for_timeout(1500)
        
        links3 = await page.query_selector_all('a[href*=DOCSEARCH]')
        for l in links3:
            text = await l.inner_text()
            href = await l.get_attribute('href')
            if 'Official' in text or ('Plat' not in text and href):
                return href.split('/')[-1] if href else None
        return None
    except Exception as e:
        return f"ERROR: {e}"

async def main():
    results = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context()
        page = await context.new_page()
        
        for county, base in COUNTIES.items():
            ds = await get_docsearch(page, county, base)
            results[county] = ds
            print(f'{county}: {ds}')
        
        await browser.close()
    
    with open('docsearch_ids.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f'\nSaved to docsearch_ids.json')

asyncio.run(main())
