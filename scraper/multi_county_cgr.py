"""
StackIQ - CGR Scraper - httpx with jsessionid
"""
import os, re, json, logging, httpx
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("cgr")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))
CGR_USER = os.getenv("CGR_USER", "")
CGR_PASS = os.getenv("CGR_PASS", "")

BASE = "https://tx.countygovernmentrecords.com"
LIST_URL = f"{BASE}/texas/landrecords/counties.jsp"
SEARCH_URL = f"{BASE}/texas/eagleweb/docSearch.jsp"
SEARCH_POST_URL = f"{BASE}/texas/eagleweb/docSearchPOST.jsp"
RESULTS_URL = f"{BASE}/texas/eagleweb/docSearchResults.jsp"

_ENV_COUNTIES = os.getenv("COUNTIES", "")
ALL_COUNTIES = ["Waller","Carson","Jasper","Karnes","Pecos","Van Zandt",
                "Caldwell","Calhoun","Hale","Morris","Ochiltree","Stephens","Titus"]
COUNTIES = [c.strip() for c in _ENV_COUNTIES.split(",")] if _ENV_COUNTIES.strip() else ALL_COUNTIES

# Doc type suffixes - same across all counties, only county code prefix differs
DOC_TYPE_SUFFIXES = {
    "Lis Pendens Notice":           "_LP",
    "Abstract of Judgment":         "_AJ",
    "Federal Tax Lien":             "_FT",
    "Mechanic's Lien Contract":     "_MM",
    "Mechanic's Lien with Assignment": "_MMA",
    "State Tax Lien":               "_ST",
    "Hospital Lien":                "_HL",
    "Probate":                      "_PRO",
    "Notice of Foreclosure":        "_NOF",
    "Judgment":                     "_J",
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
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

def parse_results(html, county, doc_type):
    soup = BeautifulSoup(html, "lxml")
    records = []
    # Find all rows with doc numbers
    for row in soup.find_all("tr"):
        text = row.get_text(" ", strip=True)
        # Doc num pattern: NNN-NNNNNNN
        doc_m = re.search(r"\b(\d{2,4}-\d{6,8})\b", text)
        if not doc_m: continue
        doc_num = doc_m.group(1)
        # Date
        date_m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
        filed = norm_date(date_m.group(1)) if date_m else ""
        if filed and filed < "2025-01-01": continue
        # Grantor - look for "Grantor:" label
        grantor_m = re.search(r"Grantor:\s*(.+?)(?:Grantee:|$)", text)
        owner = grantor_m.group(1).strip() if grantor_m else ""
        # Clean up owner - remove extra whitespace
        owner = re.sub(r'\s+', ' ', owner).strip()
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
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    all_records = []

    with httpx.Client(headers=hdrs, follow_redirects=True, timeout=30) as client:
        # Login with jsessionid
        r0 = client.get(f"{BASE}/texas/web/login.jsp")
        soup0 = BeautifulSoup(r0.text, "lxml")
        form = soup0.find("form")
        action = form.get("action","") if form else ""
        jsid = action.split("jsessionid=")[-1] if "jsessionid=" in action else ""
        login_url = f"{BASE}/texas/web/loginPOST.jsp" + (f";jsessionid={jsid}" if jsid else "")
        r1 = client.post(login_url, data={"userId": CGR_USER, "password": CGR_PASS, "submit": "Login"})
        log.info("Login: %s %s", r1.status_code, r1.url)

        # Get county list
        r2 = client.get(LIST_URL)
        soup2 = BeautifulSoup(r2.text, "lxml")
        
        # Build county URL map
        county_urls = {}
        for a in soup2.find_all("a", href=True):
            txt = a.get_text(strip=True)
            for county in COUNTIES:
                if county.lower() in txt.lower():
                    href = a["href"]
                    if not href.startswith("http"):
                        href = f"{BASE}/texas/landrecords/{href}" if not href.startswith("/") else BASE + href
                    county_urls[county] = href
        log.info("County URLs found: %s", list(county_urls.keys()))

        for county in COUNTIES:
            if county not in county_urls:
                log.warning("No URL for county: %s", county)
                continue

            # Select county
            r = client.get(county_urls[county])
            log.info("Selected %s -> %s", county, r.url)

            # Get search page to find county code from option values
            r_search = client.get(SEARCH_URL)
            soup_s = BeautifulSoup(r_search.text, "lxml")
            county_code = None
            for opt in soup_s.find_all("option"):
                val = opt.get("value","")
                if re.match(r"^\d+_LP$", val):
                    county_code = val.split("_")[0]
                    break
            if not county_code:
                log.warning("Could not find county code for %s", county)
                continue
            log.info("%s county code: %s", county, county_code)

            # Also get all available doc types for this county
            avail_opts = {opt.get_text(strip=True): opt.get("value","") for opt in soup_s.find_all("option") if opt.get("value","")}
            total_types = len(avail_opts)

            for doc_type, suffix in DOC_TYPE_SUFFIXES.items():
                search_val = f"{county_code}{suffix}"
                # Check if this doc type exists for this county
                if search_val not in avail_opts.values():
                    # Try to find by label
                    matching = [v for k,v in avail_opts.items() if doc_type.lower() in k.lower()]
                    if matching:
                        search_val = matching[0]
                    else:
                        continue

                try:
                    form_data = {
                        "DocNumID": "",
                        "RecDateIDStart": cutoff.strftime("%m/%d/%Y"),
                        "RecDateIDEnd": now.strftime("%m/%d/%Y"),
                        "BookVolPageIDBook": "", "BookVolPageIDVolume": "", "BookVolPageIDPage": "",
                        "GrantorIDSearchString": "", "GrantorIDSearchType": "Exact Match",
                        "GranteeIDSearchString": "", "GranteeIDSearchType": "Exact Match",
                        "BothNamesIDSearchString": "", "BothNamesIDSearchType": "Exact Match",
                        "docTypeTotal": str(total_types),
                        "__search_select": search_val,
                    }
                    r = client.post(SEARCH_POST_URL, data=form_data)
                    log.info("%s %s: %s", county, doc_type, r.url)

                    page_num = 1
                    while page_num <= 10:
                        recs = parse_results(r.text, county, doc_type)
                        log.info("%s %s p%d: %d records", county, doc_type, page_num, len(recs))
                        all_records.extend(recs)
                        if not recs: break
                        soup_r = BeautifulSoup(r.text, "lxml")
                        nxt = soup_r.find("a", string=re.compile(r"Next", re.I))
                        if not nxt: break
                        href = nxt["href"]
                        r = client.get(BASE + "/texas/eagleweb/" + href if not href.startswith("http") else href)
                        page_num += 1
                except Exception as e:
                    log.warning("%s %s: %s", county, doc_type, e)

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

if __name__ == "__main__":
    main()
