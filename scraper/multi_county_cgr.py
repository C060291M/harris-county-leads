"""CGR scraper template - one county per script"""
import os, re, json, logging, httpx
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("cgr")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
CGR_USER = os.getenv("CGR_USER", "")
CGR_PASS = os.getenv("CGR_PASS", "")
COUNTY = os.getenv("COUNTY", "Waller")

BASE = "https://tx.countygovernmentrecords.com"
SEARCH_POST_URL = f"{BASE}/texas/eagleweb/docSearchPOST.jsp"

DOC_TYPE_SUFFIXES = ["_LP","_AJ","_FT","_MM","_MMA","_ST","_HL","_PRO"]

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y %I:%M:%S %p","%m/%d/%Y","%Y-%m-%d","%m/%d/%y"):
        try: return datetime.strptime(str(raw).strip()[:20],fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def cat_from_doc_type(dt):
    d = dt.upper()
    if "LIS PEN" in d: return ("LP","Lis Pendens")
    if "ABSTRACT" in d or "JUDGMENT" in d: return ("JUD","Abstract of Judgment")
    if "FEDERAL" in d: return ("LNFED","Federal Tax Lien")
    if "STATE TAX" in d: return ("LNSTATE","State Tax Lien")
    if "MECHANIC" in d: return ("LNMECH","Mechanic Lien")
    if "HOSPITAL" in d: return ("LN","Hospital Lien")
    if "PROBATE" in d: return ("PRO","Probate")
    return ("LN", dt)

def compute_score(r):
    s,flags = 0,[]
    cat = r.get("cat","")
    if cat=="LNFED": flags.append("Fed Tax Lien"); s+=45
    elif cat=="JUD": flags.append("Judgment"); s+=35
    elif cat=="LNMECH": flags.append("Mech Lien"); s+=30
    elif cat=="PRO": flags.append("Probate"); s+=25
    elif cat in("LP","NOFC"): flags.append("Lis Pendens"); s+=20
    elif cat=="LNSTATE": flags.append("State Tax Lien"); s+=20
    elif cat=="LN": flags.append("Lien"); s+=15
    filed = r.get("filed","")
    if filed:
        try:
            days=(datetime.now()-datetime.strptime(filed[:10],"%Y-%m-%d")).days
            if days<=7: flags.append("New this week"); s+=10
            elif days<=30: flags.append("Filed this month"); s+=5
        except: pass
    return min(s,100),flags

def parse_results(html, county, doc_type):
    soup = BeautifulSoup(html,"lxml")
    records = []
    for row in soup.find_all("tr"):
        text = row.get_text(" ",strip=True)
        doc_m = re.search(r"\b(\d{2,4}-\d{6,8})\b",text)
        if not doc_m: continue
        doc_num = doc_m.group(1)
        date_m = re.search(r"(\d{2}/\d{2}/\d{4})",text)
        filed = norm_date(date_m.group(1)) if date_m else ""
        if filed and filed < "2025-01-01": continue
        grantor_m = re.search(r"Grantor:\s*(.+?)(?:Grantee:|$)",text)
        owner = re.sub(r'\s+',' ',grantor_m.group(1)).strip() if grantor_m else ""
        if not owner or len(owner)<3: continue
        cat,lbl = cat_from_doc_type(doc_type)
        rec = {"doc_num":doc_num,"doc_type":doc_type,"cat":cat,"cat_label":lbl,
               "filed":filed,"owner":owner,"grantee":"","amount":None,"legal":"",
               "county":county.lower(),"clerk_url":f"{BASE}/texas/eagleweb/docSearchResults.jsp",
               "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"","score":0,"flags":[]}
        rec["score"],rec["flags"] = compute_score(rec)
        records.append(rec)
    return records

def main():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    hdrs = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36",
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    all_records = []

    with httpx.Client(headers=hdrs, follow_redirects=True, timeout=30) as client:
        # Login
        r0 = client.get(f"{BASE}/texas/web/login.jsp")
        soup0 = BeautifulSoup(r0.text,"lxml")
        form = soup0.find("form")
        action = form.get("action","") if form else ""
        jsid = action.split("jsessionid=")[-1] if "jsessionid=" in action else ""
        r1 = client.post(f"{BASE}/texas/web/loginPOST.jsp;jsessionid={jsid}",
                         data={"userId":CGR_USER,"password":CGR_PASS,"submit":"Login"})
        log.info("Login: %s %s", r1.status_code, r1.url)
        if "Maximum Allowed" in r1.text:
            log.error("Too many sessions - login blocked")
            return

        # Select county
        r2 = client.get(f"{BASE}/texas/landrecords/counties.jsp;jsessionid={jsid}")
        soup2 = BeautifulSoup(r2.text,"lxml")
        link = None
        for a in soup2.find_all("a",href=True):
            if COUNTY.lower() in a.get_text().lower():
                link = a; break
        if not link:
            log.error("County not found: %s", COUNTY)
            return
        href = link["href"]
        county_url = f"{BASE}/texas/landrecords/{href}" if not href.startswith("http") else href
        r3 = client.get(county_url)
        log.info("Selected %s -> %s", COUNTY, r3.url)

        # Get county code from search page
        r_s = client.get(f"{BASE}/texas/eagleweb/docSearch.jsp")
        soup_s = BeautifulSoup(r_s.text,"lxml")
        county_code = None
        avail_vals = set()
        for opt in soup_s.find_all("option"):
            v = opt.get("value","")
            avail_vals.add(v)
            if re.match(r"^\d+_LP$",v):
                county_code = v.split("_")[0]
        if not county_code:
            log.error("Could not get county code for %s", COUNTY)
            return
        log.info("%s county code: %s, %d doc types", COUNTY, county_code, len(avail_vals))

        for suffix in DOC_TYPE_SUFFIXES:
            search_val = f"{county_code}{suffix}"
            if search_val not in avail_vals: continue
            doc_type = next((opt.get_text(strip=True) for opt in soup_s.find_all("option") if opt.get("value","")==search_val), suffix)
            try:
                form_data = {
                    "DocNumID":"","RecDateIDStart":cutoff.strftime("%m/%d/%Y"),
                    "RecDateIDEnd":now.strftime("%m/%d/%Y"),
                    "BookVolPageIDBook":"","BookVolPageIDVolume":"","BookVolPageIDPage":"",
                    "GrantorIDSearchString":"","GrantorIDSearchType":"Exact Match",
                    "GranteeIDSearchString":"","GranteeIDSearchType":"Exact Match",
                    "BothNamesIDSearchString":"","BothNamesIDSearchType":"Exact Match",
                    "docTypeTotal":str(len(avail_vals)),"__search_select":search_val,
                }
                r = client.post(SEARCH_POST_URL, data=form_data)
                log.info("%s %s response: %d bytes, url=%s", COUNTY, doc_type, len(r.text), r.url)
                if len(r.text) < 2000:
                    log.info("Short response: %s", r.text[:300].replace("\n"," "))
                page_num = 1
                while page_num <= 10:
                    recs = parse_results(r.text, COUNTY, doc_type)
                    log.info("%s %s p%d: %d records", COUNTY, doc_type, page_num, len(recs))
                    all_records.extend(recs)
                    if not recs: break
                    soup_r = BeautifulSoup(r.text,"lxml")
                    nxt = soup_r.find("a", string=re.compile(r"Next",re.I))
                    if not nxt: break
                    href = nxt["href"]
                    r = client.get(f"{BASE}/texas/eagleweb/{href}" if not href.startswith("http") else href)
                    page_num += 1
            except Exception as e:
                log.warning("%s %s: %s", COUNTY, doc_type, e)

    seen,deduped = set(),[]
    for rec in all_records:
        k = f"{rec['doc_num']}|{rec['county']}"
        if k not in seen: seen.add(k); deduped.append(rec)
    log.info("Total unique: %d", len(deduped))
    slug = COUNTY.lower().replace(" ","_")
    os.makedirs("dashboard",exist_ok=True)
    with open(f"dashboard/cgr_{slug}_records.json","w") as f:
        json.dump({"fetched_at":now.isoformat(),"source":"CGR","total":len(deduped),
                   "counties":[COUNTY],"records":deduped},f,indent=2,default=str)
    log.info("Saved -> dashboard/cgr_%s_records.json", slug)

if __name__ == "__main__":
    main()
