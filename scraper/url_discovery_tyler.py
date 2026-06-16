"""
Tyler iDS URL Discovery - enumerate all 254 TX counties
Tests both countytx-web.tylerhost.net and tx-web.tylerhost.net patterns
"""
import asyncio, json, re
from playwright.async_api import async_playwright

# All 254 Texas counties (normalized for URL testing)
ALL_COUNTIES = [
    "anderson","andrews","angelina","aransas","archer","armstrong","atascosa","austin",
    "bailey","bandera","bastrop","baylor","bee","bell","bexar","blanco","borden","bosque",
    "bowie","brazoria","brazos","brewster","briscoe","brooks","brown","burleson","burnet",
    "caldwell","calhoun","callahan","cameron","camp","carson","cass","castro","chambers",
    "cherokee","childress","clay","cochran","coke","coleman","collin","collingsworth",
    "colorado","comal","comanche","concho","cooke","coryell","cottle","crane","crockett",
    "crosby","culberson","dallam","dallas","dawson","deaf-smith","delta","denton","dewitt",
    "dickens","dimmit","donley","duval","eastland","ector","edwards","ellis","elpaso",
    "erath","falls","fannin","fayette","fisher","floyd","foard","fortbend","franklin",
    "freestone","frio","gaines","galveston","garza","gillespie","glasscock","goliad",
    "gonzales","gray","grayson","gregg","grimes","guadalupe","hale","hall","hamilton",
    "hansford","hardeman","hardin","harris","harrison","hartley","haskell","hays",
    "hemphill","henderson","hidalgo","hill","hockley","hood","hopkins","houston",
    "howard","hudspeth","hunt","hutchinson","irion","jack","jackson","jasper","jeffdavis",
    "jefferson","jimhogg","jimwells","johnson","jones","karnes","kaufman","kendall",
    "kenedy","kent","kerr","kimble","king","kinney","kleberg","knox","lamar","lamb",
    "lampasas","lasalle","lavaca","lee","leon","liberty","limestone","lipscomb","liveoak",
    "llano","loving","lubbock","lynn","madison","marion","martin","mason","matagorda",
    "maverick","mcculloch","mclennan","mcmullen","medina","menard","midland","milam",
    "mills","mitchell","montague","montgomery","moore","morris","motley","nacogdoches",
    "navarro","newton","nolan","nueces","ochiltree","oldham","orange","palopinto","panola",
    "parker","parmer","pecos","polk","potter","presidio","rains","randall","reagan","real",
    "redriver","reeves","refugio","roberts","robertson","rockwall","runnels","rusk",
    "sabine","sanaugustine","sanjacinto","sanpatricio","sansaba","schleicher","scurry",
    "shackelford","shelby","sherman","smith","somervell","starr","stephens","sterling",
    "stonewall","sutton","swisher","tarrant","taylor","terrell","terry","throckmorton",
    "titus","tomgreen","travis","trinity","tyler","upton","uvalde","valverde","vanzandt",
    "victoria","walker","waller","ward","washington","webb","wharton","wheeler","wichita",
    "wilbarger","willacy","williamson","wilson","winkler","wise","wood","yoakum","young",
    "zapata","zavala"
]

# Already confirmed working
CONFIRMED = {
    "rockwall","kaufman","delta","hood","henderson","hunt","navarro","vanzandt","hill",
    "lamar","gregg","harrison","upshur","wood","eastland","erath","liberty","hardin",
    "orange","jasper","hays","comal","guadalupe","bastrop","burnet","gonzales","karnes",
    "calhoun","wichita","howard","andrews","palopinto","wise","bowie","polk","winkler",
    "yoakum","somervell","hamilton","mills","mclennan","aransas","carson","colorado",
    "dallam","donley","kimble","waller","taylor","ector","randall","lavaca","panola",
    "tomgreen","lubbock","anderson","bee","sanpatricio","starr","milam","kendall",
    "gillespie","walker","matagorda","chambers","reeves","blanco","grimes","jimwells",
    "freestone","burleson","leon","zapata","goliad","rockwall","navarro","colorado",
    "wilson","nacogdoches","grayson","midland","bexar","hidalgo","cameron","jefferson",
    "nueces","johnson","smith","collin","denton","harris","brazos","dallas","tarrant"
}

async def check_url(session, county):
    results = []
    for pattern in [
        f"https://{county}countytx-web.tylerhost.net/web",
        f"https://{county}tx-web.tylerhost.net/web",
        f"https://{county}county-web.tylerhost.net/web",
    ]:
        try:
            page = await session.new_page()
            await page.goto(pattern, timeout=8000, wait_until='domcontentloaded')
            url = page.url
            title = await page.title()
            await page.close()
            if 'tylerhost' in url or 'Self-Service' in title or 'Clerk' in title:
                results.append({'county': county, 'url': pattern, 'final_url': url, 'title': title})
                print(f"  FOUND: {county} -> {pattern}")
                break
        except:
            try: await page.close()
            except: pass
    return results

async def main():
    found = []
    # Only test counties not already confirmed
    to_test = [c for c in ALL_COUNTIES if c not in CONFIRMED and c.replace('-','') not in CONFIRMED]
    print(f"Testing {len(to_test)} unconfirmed counties...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox','--disable-dev-shm-usage'])
        context = await browser.new_context()
        
        # Test in batches of 10 concurrent
        batch_size = 10
        for i in range(0, len(to_test), batch_size):
            batch = to_test[i:i+batch_size]
            tasks = [check_url(context, c) for c in batch]
            results = await asyncio.gather(*tasks)
            for r in results:
                found.extend(r)
            print(f"Progress: {min(i+batch_size, len(to_test))}/{len(to_test)}, found so far: {len(found)}")
        
        await browser.close()
    
    print(f"\n=== RESULTS: {len(found)} new Tyler portals found ===")
    for r in found:
        print(f"  {r['county']}: {r['url']}")
    
    with open('tyler_discovery_results.json', 'w') as f:
        json.dump(found, f, indent=2)
    print("Saved to tyler_discovery_results.json")

asyncio.run(main())
