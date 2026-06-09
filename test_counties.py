import requests, time

# All 254 Texas counties minus ones we already scrape
already_scraping = {
    "harris","dallas","tarrant","bexar","collin","denton","montgomery",
    "williamson","hidalgo","elpaso","nueces","jefferson","brazos","bee",
    "midland","potter","cameron","wilson","milam","johnson","anderson",
    "nacogdoches","chambers","leon","freestone","burleson","grimes",
    "walker","madison","matagorda","refugio","starr","zapata","medina",
    "gillespie","llano","blanco","kendall","grayson","brewster","coleman",
    "goliad","hockley","reagan","reeves","young","travis","bell",
    "rusk","panola","cherokee","houston","trinity","randall","tomgreen",
    "taylor","ector","lubbock","victoria","calhoun","jackson","wharton",
    "lavaca","bosque","coryell","falls","robertson","limestone","smith"
}

# Remaining Texas counties to test
remaining = [
    "aransas","archer","armstrong","atascosa","austin","bailey","bandera",
    "bastrop","baylor","borden","bowie","brazoria","briscoe","brooks","brown",
    "callahan","camp","carson","cass","castro","childress","clay","cochran",
    "coke","collingsworth","colorado","comal","comanche","concho","cooke",
    "cottle","crane","crockett","crosby","culberson","dawson","deaf smith",
    "delta","dewitt","dickens","dimmit","donley","duval","eastland","edwards",
    "ellis","erath","fannin","fayette","fisher","floyd","foard","franklin",
    "frio","gaines","galveston","garza","glasscock","gonzales","gray",
    "gregg","guadalupe","hale","hall","hamilton","hansford","hardeman",
    "hardin","hartley","haskell","hays","henderson","hill","hood","hopkins",
    "howard","hudspeth","hunt","hutchinson","irion","jack","jasper","jim hogg",
    "jim wells","jones","karnes","kaufman","kendall","kenedy","kent","kerr",
    "kimble","king","kinney","kleberg","knox","la salle","lamar","lamb",
    "lampasas","lee","liberty","lipscomb","live oak","llano","loving","lubbock",
    "lynn","madison","marion","martin","mason","maverick","mcculloch",
    "mclennan","mcmullen","menard","midland","mills","mitchell","montague",
    "moore","morris","motley","navarro","newton","nolan","ochiltree","oldham",
    "orange","palo pinto","parmer","pecos","polk","presidio","rains","real",
    "red river","roberts","rockwall","runnels","sabine","san augustine",
    "san jacinto","san patricio","san saba","schleicher","scurry","shackelford",
    "shelby","sherman","somervell","stephens","sterling","stonewall","sutton",
    "swisher","terrell","terry","throckmorton","titus","tom green","travis",
    "tyler","upshur","upton","uvalde","val verde","van zandt","ward",
    "washington","webb","wharton","wheeler","wichita","wilbarger","willacy",
    "winkler","wise","wood","yoakum","zavala"
]

# Normalize county name to subdomain format
def to_subdomain(name):
    return name.lower().replace(" ", "").replace("'","")

live = []
dead = []

print(f"Testing {len(remaining)} counties...")
for county in remaining:
    sub = to_subdomain(county)
    if sub in already_scraping:
        continue
    url = f"https://{sub}.tx.publicsearch.us"
    try:
        r = requests.get(url, timeout=8, allow_redirects=True)
        if r.status_code < 400:
            live.append((county, url))
            print(f"  LIVE: {county} -> {url}")
        else:
            dead.append(county)
    except Exception as e:
        dead.append(county)
    time.sleep(0.3)

print(f"\n=== RESULTS ===")
print(f"Live: {len(live)}")
print(f"Dead: {len(dead)}")
print(f"\nLive counties:")
for c, u in live:
    print(f"  {c}: {u}")
