"""
StackIQ — Multi-County Texas Lead Scraper
Scrapes motivated seller leads from top Texas county clerk portals.
Runs via GitHub Actions daily alongside Harris County scraper.
Pushes all records to PostgreSQL via api.stackiq.org.

Counties covered:
  - Dallas    (dallascounty.org)
  - Tarrant   (tarrantcounty.com)
  - Bexar     (bexar.tx.publicsearch.us)
  - Travis    (deed.traviscountytx.gov)
  - Collin    (collincountytx.gov)
"""

import csv, io, json, logging, re, time, os
from datetime import datetime, timedelta
from urllib.parse import urlencode, unquote
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("multi_county")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))
API_URL       = os.getenv("API_URL", "https://api.stackiq.org/leads/bulk-import")

MOTIVATED_TYPES = {
    "TAX DEED":               ("TAXDEED",  "Tax Deed"),
    "TAX LIEN":               ("TAXLIEN",  "Tax Lien"),
    "IRS LIEN":               ("LNIRS",    "IRS Lien"),
    "FEDERAL TAX LIEN":       ("LNFED",    "Federal Tax Lien"),
    "STATE TAX LIEN":         ("LNSTATE",  "State Tax Lien"),
    "NOTICE OF FORECLOSURE":  ("NOFC",     "Notice of Foreclosure"),
    "NOTICE OF DEFAULT":      ("NOFD",     "Notice of Default"),
    "LIS PENDENS":            ("LP",       "Lis Pendens"),
    "LIS PENDEN":             ("LP",       "Lis Pendens"),
    "MECHANIC LIEN":          ("LNMECH",   "Mechanic Lien"),
    "MECHANICS LIEN":         ("LNMECH",   "Mechanic Lien"),
    "HOA LIEN":               ("LNHOA",    "HOA Lien"),
    "LIEN":                   ("LN",       "Lien"),
    "ABSTRACT OF JUDGMENT":   ("JUD",      "Abstract of Judgment"),
    "JUDGMENT":               ("JUD",      "Judgment"),
    "PROBATE":                ("PRO",      "Probate"),
    "LETTERS TESTAMENTARY":   ("PRO",      "Probate"),
    "DIVORCE":                ("DIV",      "Divorce"),
    "BANKRUPTCY":             ("BK",       "Bankruptcy"),
}

def classify(raw_type):
    t = raw_type.upper().strip()
    for key, val in MOTIVATED_TYPES.items():
        if key in t:
            return val
    return None, None

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y","%d/%m/%Y","%Y%m%d"):
        try: return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return raw.strip()

def parse_amount(raw):
    if not raw: return None
    cleaned = re.sub(r"[^\d.]", "", str(raw))
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except ValueError: return None

def compute_score(r, cutoff):
    s, flags = 0, []
    cat = r.get("cat","")
    if cat in ("TAXDEED","TAXLIEN","LNIRS","LNFED"): flags.append("Tax Deed / IRS / Corp Lien"); s += 30
    if cat in ("LNHOA",): flags.append("HOA / Mechanic Lien"); s += 25
    if cat in ("PRO",): flags.append("Probate / Estate"); s += 20
    if cat in ("LN","LNMECH","LNSTATE"): flags.append("Lien on record"); s += 15
    if cat in ("LP","NOFC","NOFD"): flags.append("Lis Pendens / Pre-foreclosure"); s += 10
    if cat in ("JUD",): flags.append("Judgment Lien"); s += 10
    if cat in ("DIV","BK"): flags.append("Divorce / Bankruptcy"); s += 10
    amt = r.get("amount")
    if amt and amt > 100000: flags.append("Amount > $100k"); s += 15
    elif amt and amt > 50000: flags.append("Amount > $50k"); s += 10
    filed_str = r.get("filed","")
    if filed_str:
        try:
            if datetime.strptime(filed_str,"%Y-%m-%d") >= cutoff:
                flags.append("New this week"); s += 5
        except ValueError: pass
    if (r.get("mail_state") or "").upper().strip() not in ("","TX"):
        flags.append("Absentee owner (out of state)"); s += 15
    if len(flags) >= 3: s += 10
    return min(s, 100), flags

def blank_rec(county, doc_num, doc_type, cat, cat_label, filed, owner,
              grantee="", amount=None, legal="", url=""):
    return {
        "doc_num": doc_num, "doc_type": doc_type, "cat": cat,
        "cat_label": cat_label, "filed": filed, "owner": owner,
        "grantee": grantee, "amount": amount, "legal": legal,
        "clerk_url": url, "county": county,
        "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
        "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
    }

def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36","Accept-Language":"en-US,en;q=0.9"})
    return s

def scrape_dallas(start, end):
    log.info("Dallas County - scraping %s to %s", start, end)
    records = []
    session = make_session()
    BASE = "https://countyclerk.dallascounty.org"
    SEARCH = f"{BASE}/DCServices/SearchResults"
    DOC_TYPES = [
        ("LIS PENDENS","LP","Lis Pendens"),
        ("TAX DEED","TAXDEED","Tax Deed"),
        ("ABSTRACT OF JUDGMENT","JUD","Abstract of Judgment"),
        ("MECHANIC'S LIEN","LNMECH","Mechanic Lien"),
        ("FEDERAL TAX LIEN","LNFED","Federal Tax Lien"),
        ("STATE TAX LIEN","LNSTATE","State Tax Lien"),
        ("HOA LIEN","LNHOA","HOA Lien"),
        ("NOTICE OF FORECLOSURE","NOFC","Notice of Foreclosure"),
        ("IRS LIEN","LNIRS","IRS Lien"),
    ]
    for doc_type, cat, cat_label in DOC_TYPES:
        try:
            params = {"SearchType":"InstrumentType","InstrumentType":doc_type,"StartDate":start,"EndDate":end,"PageSize":"200","PageNumber":"1"}
            r = session.get(SEARCH, params=params, timeout=30)
            if r.status_code != 200:
                log.warning("Dallas %s: HTTP %d", doc_type, r.status_code)
                continue
            try:
                data = r.json()
                items = data.get("results", data.get("Results", data.get("items", [])))
                for item in items:
                    fn      = str(item.get("instrumentNumber", item.get("fileNumber","")))
                    filed   = norm_date(str(item.get("fileDate", item.get("recordedDate",""))))
                    owner   = str(item.get("grantor", item.get("owner",""))).strip()
                    grantee = str(item.get("grantee","")).strip()
                    amt     = parse_amount(str(item.get("amount", item.get("consideration",""))))
                    legal   = str(item.get("legalDescription","")).strip()
                    url     = f"{BASE}/DCServices/Document/{fn}" if fn else ""
                    records.append(blank_rec("Dallas",fn,doc_type,cat,cat_label,filed,owner,grantee,amt,legal,url))
            except Exception:
                soup = BeautifulSoup(r.text,"lxml")
                for table in soup.find_all("table"):
                    rows = table.find_all("tr")
                    if len(rows) < 2: continue
                    hdrs = [td.get_text(" ",strip=True).lower() for td in rows[0].find_all(["th","td"])]
                    if not any(k in " ".join(hdrs) for k in ["instrument","grantor","file"]): continue
                    for row in rows[1:]:
                        cells = row.find_all("td")
                        if not cells: continue
                        d = {hdrs[i]:cells[i].get_text(" ",strip=True) for i in range(min(len(hdrs),len(cells)))}
                        def f(*keys):
                            for k in keys:
                                for h in hdrs:
                                    if k in h:
                                        v = d.get(h,"").strip()
                                        if v: return v
                            return ""
                        fn    = f("instrument","number","file")
                        filed = norm_date(f("date","filed","recorded"))
                        owner = f("grantor","owner","name")
                        link  = next((a["href"] if a["href"].startswith("http") else BASE+a["href"] for cell in cells for a in cell.find_all("a",href=True)),"")
                        if not fn and not owner: continue
                        records.append(blank_rec("Dallas",fn,doc_type,cat,cat_label,filed,owner,clerk_url=link))
            time.sleep(1)
        except Exception as e:
            log.warning("Dallas %s error: %s", doc_type, e)
    log.info("Dallas: %d records", len(records))
    return records


def scrape_tarrant(start, end):
    log.info("Tarrant County - scraping %s to %s", start, end)
    records = []
    session = make_session()
    BASE   = "https://www.tarrantcounty.com"
    SEARCH = f"{BASE}/content/main/en/county-clerk/land-records/search-results.html"
    DOC_TYPES = [
        ("LIS PENDENS","LP","Lis Pendens"),
        ("TAX DEED","TAXDEED","Tax Deed"),
        ("ABSTRACT OF JUDGMENT","JUD","Abstract of Judgment"),
        ("MECHANIC LIEN","LNMECH","Mechanic Lien"),
        ("FEDERAL TAX LIEN","LNFED","Federal Tax Lien"),
        ("HOA LIEN","LNHOA","HOA Lien"),
        ("NOTICE OF FORECLOSURE","NOFC","Notice of Foreclosure"),
        ("IRS LIEN","LNIRS","IRS Lien"),
    ]
    for doc_type, cat, cat_label in DOC_TYPES:
        try:
            payload = {"startDate":start,"endDate":end,"documentType":doc_type,"pageSize":"200"}
            r = session.post(SEARCH, data=payload, timeout=30)
            if r.status_code != 200:
                r = session.get(SEARCH, params=payload, timeout=30)
            if r.status_code != 200:
                log.warning("Tarrant %s: HTTP %d", doc_type, r.status_code)
                continue
            try:
                data = r.json()
                items = data.get("results", data.get("documents", data.get("items",[])))
                for item in items:
                    fn      = str(item.get("documentNumber", item.get("instrumentNumber","")))
                    filed   = norm_date(str(item.get("recordedDate", item.get("fileDate",""))))
                    owner   = str(item.get("grantor", item.get("owner",""))).strip()
                    grantee = str(item.get("grantee","")).strip()
                    amt     = parse_amount(str(item.get("amount", item.get("consideration",""))))
                    legal   = str(item.get("legalDescription","")).strip()
                    url     = str(item.get("documentUrl", item.get("url",""))).strip()
                    records.append(blank_rec("Tarrant",fn,doc_type,cat,cat_label,filed,owner,grantee,amt,legal,url))
            except Exception:
                soup = BeautifulSoup(r.text,"lxml")
                for table in soup.find_all("table"):
                    rows = table.find_all("tr")
                    if len(rows) < 2: continue
                    hdrs = [td.get_text(" ",strip=True).lower() for td in rows[0].find_all(["th","td"])]
                    if not any(k in " ".join(hdrs) for k in ["instrument","grantor","document","file"]): continue
                    for row in rows[1:]:
                        cells = row.find_all("td")
                        if not cells: continue
                        d = {hdrs[i]:cells[i].get_text(" ",strip=True) for i in range(min(len(hdrs),len(cells)))}
                        def f(*keys):
                            for k in keys:
                                for h in hdrs:
                                    if k in h:
                                        v = d.get(h,"").strip()
                                        if v: return v
                            return ""
                        fn    = f("number","instrument","document")
                        filed = norm_date(f("date","recorded","filed"))
                        owner = f("grantor","owner","name")
                        link  = next((a["href"] if a["href"].startswith("http") else BASE+a["href"] for cell in cells for a in cell.find_all("a",href=True)),"")
                        if not fn and not owner: continue
                        records.append(blank_rec("Tarrant",fn,doc_type,cat,cat_label,filed,owner,clerk_url=link))
            time.sleep(1)
        except Exception as e:
            log.warning("Tarrant %s error: %s", doc_type, e)
    log.info("Tarrant: %d records", len(records))
    return records

def scrape_bexar(start, end):
    log.info("Bexar County - scraping %s to %s", start, end)
    records = []
    session = make_session()
    session.headers.update({"Origin":"https://bexar.tx.publicsearch.us","Referer":"https://bexar.tx.publicsearch.us/"})
    BASE = "https://bexar.tx.publicsearch.us"
    API  = f"{BASE}/api/instruments/search"
    try:
        start_api = datetime.strptime(start,"%m/%d/%Y").strftime("%Y-%m-%d")
        end_api   = datetime.strptime(end,"%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        start_api = start; end_api = end
    DOC_CODES = [
        ("LP","LP","Lis Pendens"),
        ("LTAX","TAXLIEN","Tax Lien"),
        ("LIRS","LNIRS","IRS Lien"),
        ("LM","LNMECH","Mechanic Lien"),
        ("LH","LNHOA","HOA Lien"),
        ("JA","JUD","Abstract of Judgment"),
        ("NF","NOFC","Notice of Foreclosure"),
        ("PRO","PRO","Probate"),
        ("LF","LNFED","Federal Tax Lien"),
    ]
    for code, cat, cat_label in DOC_CODES:
        try:
            payload = {"docTypeCode":code,"startDate":start_api,"endDate":end_api,"searchType":"Document","offset":0,"limit":200,"sort":"desc"}
            r = session.post(API, json=payload, timeout=30)
            if r.status_code == 200:
                data  = r.json()
                items = data.get("results", data.get("instruments", data.get("hits",{}).get("hits",[])))
                if isinstance(items, dict): items = items.get("hits",[])
                for item in items:
                    if "_source" in item: item = item["_source"]
                    fn      = str(item.get("instrumentNumber", item.get("docNumber", item.get("id",""))))
                    filed   = norm_date(str(item.get("recordedDate", item.get("fileDate", item.get("date","")))))
                    owner   = str(item.get("grantor", item.get("grantorName", item.get("owner","")))).strip()
                    grantee = str(item.get("grantee", item.get("granteeName",""))).strip()
                    amt     = parse_amount(str(item.get("amount", item.get("consideration",""))))
                    legal   = str(item.get("legalDescription", item.get("legal",""))).strip()
                    url     = f"{BASE}/doc/{fn}" if fn else ""
                    rec = blank_rec("Bexar",fn,code,cat,cat_label,filed,owner,grantee,amt,legal,url)
                    for mail_key in ["mailAddress","mailingAddress","address"]:
                        if item.get(mail_key):
                            rec["mail_address"] = str(item[mail_key]).strip(); break
                    records.append(rec)
            else:
                log.warning("Bexar %s: HTTP %d", code, r.status_code)
            time.sleep(0.5)
        except Exception as e:
            log.warning("Bexar %s error: %s", code, e)
    log.info("Bexar: %d records", len(records))
    return records


def scrape_travis(start, end):
    log.info("Travis County - scraping %s to %s", start, end)
    records = []
    session = make_session()
    BASE   = "https://deed.traviscountytx.gov"
    SEARCH = f"{BASE}/search"
    DOC_TYPES = [
        ("LIS PENDENS","LP","Lis Pendens"),
        ("TAX DEED","TAXDEED","Tax Deed"),
        ("ABSTRACT OF JUDGMENT","JUD","Abstract of Judgment"),
        ("MECHANIC LIEN","LNMECH","Mechanic Lien"),
        ("FEDERAL TAX LIEN","LNFED","Federal Tax Lien"),
        ("HOA LIEN","LNHOA","HOA Lien"),
        ("NOTICE OF FORECLOSURE","NOFC","Notice of Foreclosure"),
        ("IRS LIEN","LNIRS","IRS Lien"),
        ("PROBATE","PRO","Probate"),
    ]
    for doc_type, cat, cat_label in DOC_TYPES:
        try:
            r = session.get(SEARCH, timeout=30)
            soup = BeautifulSoup(r.text,"lxml")
            vs = {i.get("name"):i.get("value","") for i in soup.find_all("input",type="hidden") if i.get("name")}
            payload = {**vs,"documentType":doc_type,"startDate":start,"endDate":end,"pageSize":"200"}
            r2 = session.post(SEARCH, data=payload, timeout=45)
            if r2.status_code != 200:
                log.warning("Travis %s: HTTP %d", doc_type, r2.status_code)
                continue
            try:
                data  = r2.json()
                items = data.get("results", data.get("documents",[]))
                for item in items:
                    fn    = str(item.get("documentNumber", item.get("instrumentNumber","")))
                    filed = norm_date(str(item.get("recordedDate", item.get("fileDate",""))))
                    owner = str(item.get("grantor", item.get("owner",""))).strip()
                    amt   = parse_amount(str(item.get("amount","")))
                    url   = str(item.get("url", f"{BASE}/document/{fn}"))
                    records.append(blank_rec("Travis",fn,doc_type,cat,cat_label,filed,owner,amount=amt,url=url))
            except Exception:
                soup2 = BeautifulSoup(r2.text,"lxml")
                for table in soup2.find_all("table"):
                    rows = table.find_all("tr")
                    if len(rows) < 2: continue
                    hdrs = [td.get_text(" ",strip=True).lower() for td in rows[0].find_all(["th","td"])]
                    if not any(k in " ".join(hdrs) for k in ["instrument","grantor","document","file"]): continue
                    for row in rows[1:]:
                        cells = row.find_all("td")
                        if not cells: continue
                        d = {hdrs[i]:cells[i].get_text(" ",strip=True) for i in range(min(len(hdrs),len(cells)))}
                        def f(*keys):
                            for k in keys:
                                for h in hdrs:
                                    if k in h:
                                        v = d.get(h,"").strip()
                                        if v: return v
                            return ""
                        fn    = f("number","instrument","document")
                        filed = norm_date(f("date","recorded","filed"))
                        owner = f("grantor","owner","name")
                        link  = next((a["href"] if a["href"].startswith("http") else BASE+a["href"] for cell in cells for a in cell.find_all("a",href=True)),"")
                        if not fn and not owner: continue
                        records.append(blank_rec("Travis",fn,doc_type,cat,cat_label,filed,owner,clerk_url=link))
            time.sleep(1)
        except Exception as e:
            log.warning("Travis %s error: %s", doc_type, e)
    log.info("Travis: %d records", len(records))
    return records


def scrape_collin(start, end):
    log.info("Collin County - scraping %s to %s", start, end)
    records = []
    session = make_session()
    BASE   = "https://www.collincountytx.gov"
    SEARCH = f"{BASE}/county_clerk/pages/online_search.aspx"
    DOC_TYPES = [
        ("LIS PENDENS","LP","Lis Pendens"),
        ("TAX DEED","TAXDEED","Tax Deed"),
        ("ABSTRACT OF JUDGMENT","JUD","Abstract of Judgment"),
        ("MECHANIC LIEN","LNMECH","Mechanic Lien"),
        ("FEDERAL TAX LIEN","LNFED","Federal Tax Lien"),
        ("HOA LIEN","LNHOA","HOA Lien"),
        ("NOTICE OF FORECLOSURE","NOFC","Notice of Foreclosure"),
        ("IRS LIEN","LNIRS","IRS Lien"),
    ]
    for doc_type, cat, cat_label in DOC_TYPES:
        try:
            r = session.get(SEARCH, timeout=30)
            soup = BeautifulSoup(r.text,"lxml")
            vs = {i.get("name"):i.get("value","") for i in soup.find_all("input",type="hidden") if i.get("name")}
            payload = {**vs,"__EVENTTARGET":"","__EVENTARGUMENT":"","ctl00$MainContent$txtDocumentType":doc_type,"ctl00$MainContent$txtStartDate":start,"ctl00$MainContent$txtEndDate":end,"ctl00$MainContent$btnSearch":"Search"}
            r2 = session.post(SEARCH, data=payload, timeout=45)
            if r2.status_code != 200:
                log.warning("Collin %s: HTTP %d", doc_type, r2.status_code)
                continue
            soup2 = BeautifulSoup(r2.text,"lxml")
            for table in soup2.find_all("table"):
                rows = table.find_all("tr")
                if len(rows) < 2: continue
                hdrs = [td.get_text(" ",strip=True).lower() for td in rows[0].find_all(["th","td"])]
                if not any(k in " ".join(hdrs) for k in ["instrument","grantor","document","file"]): continue
                for row in rows[1:]:
                    cells = row.find_all("td")
                    if not cells: continue
                    d = {hdrs[i]:cells[i].get_text(" ",strip=True) for i in range(min(len(hdrs),len(cells)))}
                    def f(*keys):
                        for k in keys:
                            for h in hdrs:
                                if k in h:
                                    v = d.get(h,"").strip()
                                    if v: return v
                        return ""
                    fn    = f("number","instrument","document","file")
                    filed = norm_date(f("date","recorded","filed"))
                    owner = f("grantor","owner","name")
                    amt   = parse_amount(f("amount","consideration"))
                    link  = next((a["href"] if a["href"].startswith("http") else BASE+a["href"] for cell in cells for a in cell.find_all("a",href=True)),"")
                    if not fn and not owner: continue
                    records.append(blank_rec("Collin",fn,doc_type,cat,cat_label,filed,owner,amount=amt,clerk_url=link))
            time.sleep(1)
        except Exception as e:
            log.warning("Collin %s error: %s", doc_type, e)
    log.info("Collin: %d records", len(records))
    return records

def main():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start  = cutoff.strftime("%m/%d/%Y")
    end    = now.strftime("%m/%d/%Y")

    log.info("=== StackIQ Multi-County Scraper ===")
    log.info("Date range: %s to %s", start, end)

    all_records = []

    scrapers = [
        ("Dallas",  scrape_dallas),
        ("Tarrant", scrape_tarrant),
        ("Bexar",   scrape_bexar),
        ("Travis",  scrape_travis),
        ("Collin",  scrape_collin),
    ]

    for name, fn in scrapers:
        try:
            recs = fn(start, end)
            all_records.extend(recs)
            log.info("+ %s: %d records", name, len(recs))
        except Exception as e:
            log.error("x %s failed: %s", name, e)

    seen, deduped = set(), []
    for r in all_records:
        key = f"{r['county']}|{r.get('doc_num') or r.get('owner')}|{r.get('filed')}"
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    log.info("Total unique records: %d", len(deduped))

    for r in deduped:
        try: r["score"], r["flags"] = compute_score(r, cutoff)
        except Exception: r["score"] = 10; r["flags"] = []

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)

    payload = {
        "fetched_at": now.isoformat(),
        "source":     "Multi-County TX Clerk Portals",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total":      len(deduped),
        "counties":   list({r["county"] for r in deduped}),
        "records":    deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data",      exist_ok=True)
    with open("dashboard/multi_county_records.json","w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/multi_county_records.json","w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/multi_county_records.json")

    try:
        r = requests.post(API_URL, json=payload, timeout=120)
        log.info("API push: %d %s", r.status_code, r.text[:100])
    except Exception as e:
        log.warning("API push failed (non-fatal): %s", e)

    hot  = sum(1 for r in deduped if r.get("score",0) >= 70)
    warm = sum(1 for r in deduped if 40 <= r.get("score",0) < 70)
    log.info("=== Summary ===")
    log.info("Total: %d | Hot: %d | Warm: %d", len(deduped), hot, warm)
    for county in sorted({r["county"] for r in deduped}):
        count = sum(1 for r in deduped if r["county"] == county)
        log.info("  %s: %d", county, count)

if __name__ == "__main__":
    main()
