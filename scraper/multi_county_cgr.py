"""
StackIQ - County Government Records (CGR) Scraper - httpx version
Portal: tx.countygovernmentrecords.com (EagleWeb/Tyler Technologies)
"""
import os, re, json, logging, asyncio, httpx
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("cgr")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))
CGR_USER = os.getenv("CGR_USER", "")
CGR_PASS = os.getenv("CGR_PASS", "")

BASE = "https://tx.countygovernmentrecords.com"
LOGIN_URL = f"{BASE}/texas/web/loginPOST.jsp"
LIST_URL  = f"{BASE}/texas/landrecords/counties.jsp"
SEARCH_URL = f"{BASE}/texas/eagleweb/docSearch.jsp"
RESULTS_URL = f"{BASE}/texas/eagleweb/docSearchResults.jsp"

_ENV_COUNTIES = os.getenv("COUNTIES", "")
ALL_COUNTIES = [
    "Waller","Carson","Jasper","Karnes","Pecos","Van Zandt",
    "Caldwell","Calhoun","Hale","Morris","Ochiltree","Stephens","Titus"
]
COUNTIES = [c.strip() for c in _ENV_COUNTIES.split(",")] if _ENV_COUNTIES.strip() else ALL_COUNTIES

DISTRESS_DOC_TYPES = [
    "Lis Pendens Notice",
    "Abstract of Judgment",
    "Federal Tax Lien",
    "Mechanic's Lien Contract",
    "Mechanic's Lien with Assignment",
    "State Tax Lien",
    "Hospital Lien",
    "Judgment",
    "Notice of Foreclosure",
    "Probate",
]

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:20], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def cat_from_doc_type(dt):
    d = dt.upper()
    if "LIS PEN" in d: return ("LP", "Lis Pendens")
    if "ABSTRACT" in d or "JUDGMENT" in d: return ("JUD", "Abstract of Judgment")
    if "FEDERAL" in d: return ("LNFED", "Federal Tax Lien")
    if "STATE TAX" in d: return ("LNSTATE", "State Tax Lien")
    if "MECHANIC" in d: return ("LNMECH", "Mechanic Lien")
    if "HOSPITAL" in d: return ("LN", "Hospital Lien")
    if "PROBATE" in d: return ("PRO", "Probate")
    if "FORECLOSURE" in d: return ("NOFC", "Notice of Foreclosure")
    return ("LN", dt)

def compute_score(r):
    s, flags = 0, []
    cat = r.get("cat","")
    if cat == "LNFED": flags.append("Fed Tax Lien"); s += 45
    elif cat == "JUD": flags.append("Judgment"); s += 35
    elif cat == "LNMECH": flags.append("Mech Lien"); s += 30
    elif cat == "PRO": flags.append("Probate"); s += 25
    elif cat in ("LP","NOFC"): flags.append("Lis Pendens"); s += 20
    elif cat == "LNSTATE": flags.append("State Tax Lien"); s += 20
    elif cat == "LN": flags.append("Lien"); s += 15
    filed = r.get("filed","")
    if filed:
        try:
            days = (datetime.now() - datetime.strptime(filed[:10], "%Y-%m-%d")).days
            if days <= 7: flags.append("New this week"); s += 10
            elif days <= 30: flags.append("Filed this month"); s += 5
        except: pass
    return min(s, 100), flags

def parse_results_page(html, county, doc_type):
    soup = BeautifulSoup(html, "lxml")
    records = []
    # Find results table
    table = soup.find("table", class_=re.compile("result", re.I)) or soup.find("table")
    if not table: return records
    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2: continue
        text = row.get_text(" ", strip=True)
        # Find doc number (format: NNN-NNNNNNN)
        doc_m = re.search(r"\b(\d{2,4}-\d{4,8})\b", text)
        if not doc_m: continue
        doc_num = doc_m.group(1)
        # Find date
        date_m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
        filed = norm_date(date_m.group(1)) if date_m else ""
        if filed and filed < "2025-01-01": continue
        # Find grantor
        grantor_cell = row.find(string=re.compile("Grantor:", re.I))
        if grantor_cell:
            grantor_td = grantor_cell.find_parent("td") or grantor_cell.find_parent("div")
            owner = grantor_td.get_text(strip=True).replace("Grantor:", "").strip() if grantor_td else ""
        else:
            # Try to find name from cells
            names = [c.get_text(strip=True) for c in cells if len(c.get_text(strip=True)) > 5 
                     and not re.match(r"\d{2}/\d{2}/\d{4}", c.get_text(strip=True))
                     and not re.match(r"\d+-\d+", c.get_text(strip=True))]
            owner = names[0] if names else ""
        if not owner or len(owner) < 3: continue
        cat, lbl = cat_from_doc_type(doc_type)
        rec = {
            "doc_num": doc_num, "doc_type": doc_type,
            "cat": cat, "cat_label": lbl,
            "filed": filed, "owner": owner, "grantee": "",
            "amount": None, "legal": "", "county": county.lower(),
            "clerk_url": RESULTS_URL,
            "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
            "score": 0, "flags": [],
        }
        rec["score"], rec["flags"] = compute_score(rec)
        records.append(rec)
    return records

def main():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    
    all_records = []
    
    with httpx.Client(headers=headers, follow_redirects=True, timeout=30) as client:
        # Login
        r = client.post(LOGIN_URL, data={"userId": CGR_USER, "password": CGR_PASS, "submit": "Login"})
        log.info("Login: %s %s", r.status_code, r.url)
        if "login" in str(r.url).lower() and "counties" not in str(r.url).lower():
            log.error("Login failed")
            return []
        
        # Get county list to find county links
        r = client.get(LIST_URL)
        soup = BeautifulSoup(r.text, "lxml")
        county_links = {}
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True)
            for county in COUNTIES:
                if county.lower() in txt.lower():
                    county_links[county] = BASE + a["href"] if not a["href"].startswith("http") else a["href"]
        log.info("Found county links: %s", list(county_links.keys()))
        
        for county in COUNTIES:
            if county not in county_links:
                log.warning("County not found: %s", county)
                continue
            
            # Select county by clicking link
            r = client.get(county_links[county])
            log.info("Selected %s: %s", county, r.url)
            
            for doc_type in DISTRESS_DOC_TYPES:
                try:
                    # Get search page
                    r = client.get(SEARCH_URL)
                    soup = BeautifulSoup(r.text, "lxml")
                    
                    # Build search form data
                    form_data = {
                        "RecDateIDStart": cutoff.strftime("%m/%d/%Y"),
                        "RecDateIDEnd": now.strftime("%m/%d/%Y"),
                        "AllDocuments": "false",
                    }
                    
                    # Find doc type select and set value
                    select = soup.find("select", {"name": re.compile("DocType|docType|document", re.I)})
                    if select:
                        for opt in select.find_all("option"):
                            if opt.get_text(strip=True) == doc_type:
                                form_data[select.get("name")] = opt.get("value", opt.get_text(strip=True))
                                break
                    
                    # Submit search
                    r = client.post(SEARCH_URL, data=form_data)
                    log.info("%s %s: %s", county, doc_type, r.url)
                    
                    recs = parse_results_page(r.text, county, doc_type)
                    log.info("%s %s: %d records", county, doc_type, len(recs))
                    all_records.extend(recs)
                    
                    # Handle pagination
                    page_num = 2
                    while page_num <= 10:
                        soup2 = BeautifulSoup(r.text, "lxml")
                        next_link = soup2.find("a", string=re.compile("Next", re.I))
                        if not next_link: break
                        r = client.get(BASE + next_link["href"] if not next_link["href"].startswith("http") else next_link["href"])
                        more = parse_results_page(r.text, county, doc_type)
                        if not more: break
                        all_records.extend(more)
                        page_num += 1
                        
                except Exception as e:
                    log.warning("%s %s: %s", county, doc_type, e)
    
    # Deduplicate
    seen, deduped = set(), []
    for rec in all_records:
        k = f"{rec['doc_num']}|{rec['county']}"
        if k not in seen:
            seen.add(k); deduped.append(rec)
    
    log.info("Total unique: %d", len(deduped))
    os.makedirs("dashboard", exist_ok=True)
    with open("dashboard/cgr_records.json", "w") as f:
        json.dump({"fetched_at": now.isoformat(), "source": "CGR", "total": len(deduped),
                   "counties": COUNTIES, "records": deduped}, f, indent=2, default=str)
    log.info("Saved -> dashboard/cgr_records.json")
    return deduped

if __name__ == "__main__":
    main()
