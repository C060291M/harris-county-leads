import asyncio, json, re
from playwright.async_api import async_playwright

# Counties with confirmed working URLs from our scrapers
COUNTIES = {
    "hill":      "https://hillcountytx-web.tylerhost.net/web",
    "hunt":      "https://huntcountytx-web.tylerhost.net/web", 
    "henderson": "https://hendersoncountytx-web.tylerhost.net/web",
    "hood":      "https://hoodcountytx-web.tylerhost.net/web",
    "lamar":     "https://lamarcountytx-web.tylerhost.net/web",
    "lavaca":    "https://lavacacountytx-web.tylerhost.net/web",
    "liberty":   "https://libertycountytx-web.tylerhost.net/web",
    "gregg":     "https://greggcountytx-web.tylerhost.net/web",
    "harrison":  "https://harrisoncountytx-web.tylerhost.net/web",
    "wood":      "https://woodcountytx-web.tylerhost.net/web",
    "upshur":    "https://upshurcountytx-web.tylerhost.net/web",
    "vanzandt":  "https://vanzandtcountytx-web.tylerhost.net/web",
    "kaufman":   "https://kaufmancountytx-web.tylerhost.net/web",
    "rockwall":  "https://rockwalltx-web.tylerhost.net/web",
    "bastrop":   "https://bastroptx-web.tylerhost.net/web",
    "hays":      "https://hayscountytx-web.tylerhost.net/web",
    "comal":     "https://comalcountytx-web.tylerhost.net/web",
    "guadalupe": "https://guadalupecountytx-web.tylerhost.net/web",
    "gonzales":  "https://gonzalescountytx-web.tylerhost.net/web",
    "karnes":    "https://karnescountytx-web.tylerhost.net/web",
    "wichita":   "https://wichitacountytx-web.tylerhost.net/web",
    "howard":    "https://howardcountytx-web.tylerhost.net/web",
    "andrews":   "https://andrewscountytx-web.tylerhost.net/web",
    "wise":      "https://wisecountytx-web.tylerhost.net/web",
    "bowie":     "https://bowiecountytx-web.tylerhost.net/web",
    "polk":      "https://polkcountytx-web.tylerhost.net/web",
    "somervell": "https://somervellcountytx-web.tylerhost.net/web",
    "hamilton":  "https://hamiltoncountytx-web.tylerhost.net/web",
    "mills":     "https://millscountytx-web.tylerhost.net/web",
    "mclennan":  "https://mclennancountytx-web.tylerhost.net/web",
    "orange":    "https://orangecountytx-web.tylerhost.net/web",
    "hardin":    "https://hardincountytx-web.tylerhost.net/web",
    "jasper":    "https://jaspercountytx-web.tylerhost.net/web",
    "panola":    "https://panolacountytx-web.tylerhost.net/web",
    "kimble":    "https://kimblecountytx-web.tylerhost.net/web",
    "randall":   "https://randallcountytx-web.tylerhost.net/web",
    "waller":    "https://wallercountytx-web.tylerhost.net/web",
    "yoakum":    "https://yoakumcountytx-selfservice.tylerhost.net/web",
    "palopinto": "https://palopintocountytx-selfservice.tylerhost.net/web",
    "eastland":  "https://eastlandcountytx-web.tylerhost.net/web",
    "aransas":   "https://aransascountytx-web.tylerhost.net/web",
    "carson":    "https://carsoncountytx-web.tylerhost.net/web",
    "dallam":    "https://dallamcountytx-web.tylerhost.net/web",
    "donley":    "https://donleycountytx-web.tylerhost.net/web",
    "kimble":    "https://kimblecountytx-web.tylerhost.net/web",
    "winkler":   "https://winklercountytx-web.tylerhost.net/web",
}

async def get_docsearch(county, base):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = await browser.new_page(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36')
        try:
            r = await page.goto(base + '/user/disclaimer', timeout=15000, wait_until='domcontentloaded')
            if not r or r.status >= 400:
                return None, "HTTP error"
            await page.wait_for_timeout(1500)
            await page.evaluate("(() => { const b = document.querySelector('button'); if(b){ b.removeAttribute('disabled'); b.click(); } })()")
            await page.wait_for_timeout(1500)
            
            # Find action group for Official Records
            links = await page.query_selector_all('a[href]')
            action_url = None
            for l in links:
                href = await l.get_attribute('href') or ''
                text = (await l.inner_text()).strip()
                if 'ACTIONGROUP' in href and ('Official' in text or 'Record' in text):
                    action_url = base + href if href.startswith('/') else href
                    break
                if 'DOCSEARCH' in href and 'Plat' not in text:
                    ds = href.split('/')[-1]
                    await browser.close()
                    return ds, "direct"
            
            if action_url:
                await page.goto(action_url, timeout=15000, wait_until='domcontentloaded')
                await page.wait_for_timeout(1500)
                links2 = await page.query_selector_all('a[href*=DOCSEARCH]')
                for l in links2:
                    href = await l.get_attribute('href') or ''
                    text = (await l.inner_text()).strip()
                    if 'Plat' not in text and 'DOCSEARCH' in href:
                        ds = href.split('/')[-1]
                        await browser.close()
                        return ds, "via action"
            
            await browser.close()
            return None, "not found"
        except Exception as e:
            await browser.close()
            return None, str(e)[:50]

async def main():
    results = {}
    for county, base in COUNTIES.items():
        ds, status = await get_docsearch(county, base)
        results[county] = {"docsearch": ds, "base_url": base, "status": status}
        icon = "✓" if ds else "✗"
        print(f"{icon} {county:<15} {ds or status}")
    
    with open('docsearch_ids.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    found = sum(1 for v in results.values() if v['docsearch'])
    print(f"\nFound DOCSEARCH IDs: {found}/{len(results)}")

asyncio.run(main())
