import urllib.request, json, re, time, os
from concurrent.futures import ThreadPoolExecutor, as_completed

ALL_TX_COUNTIES = [
    'anderson','andrews','angelina','aransas','archer','armstrong','atascosa','austin',
    'bailey','bandera','bastrop','baylor','bee','bell','bexar','blanco','borden','bosque',
    'bowie','brazoria','brazos','brewster','briscoe','brooks','brown','burleson','burnet',
    'caldwell','calhoun','callahan','cameron','camp','carson','cass','castro','chambers',
    'cherokee','childress','clay','cochran','coke','coleman','collin','collingsworth',
    'colorado','comal','comanche','concho','cooke','coryell','cottle','crane','crockett',
    'crosby','culberson','dallam','dallas','dawson','deaf smith','delta','denton','dewitt',
    'dickens','dimmit','donley','duval','eastland','ector','edwards','ellis','elpaso',
    'erath','falls','fannin','fayette','fisher','floyd','foard','fortbend','franklin',
    'freestone','frio','gaines','galveston','garza','gillespie','glasscock','goliad',
    'gonzales','gray','grayson','gregg','grimes','guadalupe','hale','hall','hamilton',
    'hansford','hardeman','hardin','harris','harrison','hartley','haskell','hays','hemphill',
    'henderson','hidalgo','hill','hockley','hood','hopkins','houston','howard','hudspeth',
    'hunt','hutchinson','irion','jack','jackson','jasper','jeffdavis','jefferson','jimhogg',
    'jimwells','johnson','jones','karnes','kaufman','kendall','kenedy','kent','kerr',
    'kimble','king','kinney','kleberg','knox','lamar','lamb','lampasas','lasalle','lavaca',
    'lee','leon','liberty','limestone','lipscomb','liveoak','llano','loving','lubbock',
    'lynn','madison','marion','martin','mason','matagorda','maverick','mcculloch','mclennan',
    'mcmullen','medina','menard','midland','milam','mills','mitchell','montague','montgomery',
    'moore','morris','motley','nacogdoches','navarro','newton','nolan','nueces','ochiltree',
    'oldham','orange','palo pinto','panola','parker','parmer','pecos','polk','potter',
    'presidio','rains','randall','reagan','real','redriver','reeves','refugio','roberts',
    'robertson','rockwall','runnels','rusk','sabine','sanaugustine','sanjacinto','sanpatrick',
    'schleicher','scurry','shackelford','shelby','sherman','smith','somervell','starr',
    'stephens','sterling','stonewall','sutton','swisher','tarrant','taylor','terrell','terry',
    'throckmorton','titus','tomgreen','travis','trinity','tyler','upshur','upton','uvalde',
    'valverde','victoria','walker','waller','ward','washington','webb','wharton','wheeler',
    'wichita','wilbarger','willacy','williamson','wilson','winkler','wise','wood','yoakum',
    'young','zapata','zavala'
]

PATTERNS = [
    ('pubsearch',   'https://{county}.tx.publicsearch.us'),
    ('tyler_ids',   'https://{county}tx-web.tylerhost.net/web'),
    ('tyler_ids2',  'https://{county}countytx-web.tylerhost.net/web'),
    ('eagleweb',    'https://{county}tx.countygovernmentrecords.com/{County}TXRecorder/web/'),
    ('eagleweb2',   'https://erecord.{county}county.gov/recorder/web/'),
    ('i2i',         'https://i2i.uslandrecords.com/TX/{County}/D/'),
    ('govos',       'https://{county}.tx.govos.com'),
]

KEYWORDS = {
    'pubsearch': ['publicsearch', 'neumo', 'Official Record Search'],
    'tyler_ids':  ['Tyler', 'iDS', 'Official Records Search'],
    'eagleweb':   ['EagleWeb', 'countygovernmentrecords'],
    'i2i':        ['uslandrecords', 'i2i', 'Land Records'],
    'govos':      ['GovOS', 'govos'],
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
}

def test_url(county, platform, url):
    try:
        req = urllib.request.Request(url, headers=headers)
        resp = urllib.request.urlopen(req, timeout=6)
        content = resp.read(2000).decode('utf-8', errors='ignore')
        if resp.status == 200 and len(content) > 100:
            return {'county': county, 'platform': platform, 'url': url, 'status': 'live'}
    except:
        pass
    return None

def discover_county(county):
    results = []
    County = county.replace(' ','').title()
    county_slug = county.replace(' ','').lower()
    for platform, pattern in PATTERNS:
        url = pattern.format(county=county_slug, County=County)
        result = test_url(county, platform, url)
        if result:
            results.append(result)
    return results

print(f'Testing {len(ALL_TX_COUNTIES)} counties against {len(PATTERNS)} platforms...')
print('This will take 3-5 minutes...')

registry = []
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(discover_county, c): c for c in ALL_TX_COUNTIES}
    done = 0
    for future in as_completed(futures):
        results = future.result()
        if results:
            for r in results:
                registry.append(r)
                print(f'  LIVE: {r["county"]:<20} {r["platform"]:<15} {r["url"]}')
        done += 1
        if done % 25 == 0:
            print(f'  Progress: {done}/{len(ALL_TX_COUNTIES)} counties tested...')

print(f'\n=== DISCOVERY COMPLETE ===')
print(f'Live portals found: {len(registry)}')
print(f'Counties with portals: {len(set(r["county"] for r in registry))}')

# Save registry
with open('county_registry.json', 'w') as f:
    json.dump(registry, f, indent=2)
print('Saved to county_registry.json')
