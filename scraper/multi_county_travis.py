import os, re, json, logging, httpx
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("travis")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "10"))
COUNTY        = "travis"
BASE          = "https://www.tccsearch.org"
SEARCH_URL    = f"{BASE}/RealEstate/SearchEntry.aspx"
RESULTS_URL   = f"{BASE}/RealEstate/SearchResults.aspx"

PROXY_USER = os.getenv("DECODO_USER", "")
PROXY_PASS = os.getenv("DECODO_PASS", "")
PROXY_URL  = f"http://{PROXY_USER}:{PROXY_PASS}@state.decodo.com:14001" if PROXY_USER else None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Hardcoded from form inspection: checkbox index -> (value, cat, label)
DOC_TYPES = {
    63: ("LIS PEND",    "LP",      "Lis Pendens"),
    1:  ("AJ",          "JUD",     "Abstract of Judgment"),
    51: ("FED TAX",     "LNFED",   "Federal Tax Lien"),
    65: ("ML",          "LNMECH",  "Mechanic Lien"),
    25: ("PROB",        "PRO",     "Probate"),
    107:("ST TAX LIEN", "LNSTATE", "State Tax Lien"),
    72: ("FORECLOSURE", "NOFC",    "Notice of Foreclosure"),
    24: ("JUDGMT",      "JUD",     "Abstract of Judgment"),
    60: ("JDGMT",       "JUD",     "Abstract of Judgment"),
    62: ("LIEN",        "LN",      "Lien"),
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def get_vs(soup):
    out = {}
    for fld in ["__VIEWSTATE","__EVENTVALIDATION","__VIEWSTATEGENERATOR"]:
        el = soup.find("input", {"name": fld})
        out[fld] = el.get("value","") if el else ""
    return out

def parse_results(soup):
    records = []
    for t in soup.find_all("table"):
        rows = t.find_all("tr")
        found = False
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5: continue
            texts = [c.get_text(" ", strip=True) for c in cells]
            doc_num = texts[3].strip() if len(texts) > 3 else ""
            if not doc_num or not re.match(r'\d{4}', doc_num): continue
            found = True
            filed    = norm_date(texts[4]) if len(texts) > 4 else ""
            doc_type = texts[5].strip() if len(texts) > 5 else ""
            name_col = texts[6] if len(texts) > 6 else ""
            legal    = texts[7].strip() if len(texts) > 7 else ""
            r_match  = re.search(r'\[R\]\s*([^\[]+)', name_col)
            e_match  = re.search(r'\[E\]\s*([^\[]+)', name_col)
            grantor  = re.sub(r'\s+', ' ', r_match.group(1)).strip() if r_match else ""
            grantee  = re.sub(r'\s+', ' ', e_match.group(1)).strip() if e_match else ""
            owner    = grantee if grantee else grantor
            if not owner or len(owner) < 3: continue
            cat = "LP" if "PEND" in doc_type.upper() else "JUD" if "JUD" in doc_type.upper() or doc_type in ("AJ","JUDGMT","JDGMT") else "LNFED" if "FED" in doc_type.upper() else "LNMECH" if doc_type == "ML" else "PRO" if "PROB" in doc_type.upper() else "LNSTATE" if "ST TAX" in doc_type.upper() else "NOFC" if "FORE" in doc_type.upper() else "LN"
            cat_label = {"LP":"Lis Pendens","JUD":"Abstract of Judgment","LNFED":"Federal Tax Lien","LNMECH":"Mechanic Lien","PRO":"Probate","LNSTATE":"State Tax Lien","NOFC":"Notice of Foreclosure","LN":"Lien"}.get(cat, doc_type)
            records.append({
                "doc_num": doc_num, "doc_type": doc_type,
                "cat": cat, "cat_label": cat_label,
                "filed": filed, "owner": owner, "grantee": grantee,
                "amount": None, "legal": legal, "county": COUNTY,
                "clerk_url": RESULTS_URL,
                "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                "score": 0, "flags": [],
            })
        if found: break
    return records

def scrape():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start_str = cutoff.strftime("%m/%d/%Y")
    end_str   = now.strftime("%m/%d/%Y")
    log.info("[Travis] Scraping %s to %s proxy=%s", start_str, end_str, bool(PROXY_URL))

    kwargs = {"headers": HEADERS, "follow_redirects": True, "timeout": 30}
    if PROXY_URL: kwargs["proxy"] = PROXY_URL

    all_records = []
    with httpx.Client(**kwargs) as client:
        # Disclaimer
        r = client.get(BASE)
        soup = BeautifulSoup(r.text, "lxml")
        vs = get_vs(soup)
        r = client.post(BASE, data={**vs, "__EVENTTARGET": "ctl00$cph1$lnkAccept", "__EVENTARGUMENT": ""},
                       headers={**HEADERS, "Referer": BASE, "Content-Type": "application/x-www-form-urlencoded"})
        log.info("[Travis] Disclaimer: %s", r.status_code)

        # Search page
        r = client.get(SEARCH_URL)
        soup = BeautifulSoup(r.text, "lxml")
        vs = get_vs(soup)
        log.info("[Travis] Search page: %s VIEWSTATE=%s", r.status_code, bool(vs.get("__VIEWSTATE")))

        # Submit one search with all distress doc types checked
        form = {
            **vs,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "ctl00$cphNoMargin$f$NameSearchMode": "rdoCombine",
            "ctl00$cphNoMargin$f$drbPartyType": "",
            "cphNoMargin_f_ddcDateFiledFrom_clientState": f'{{"enabled":true,"emptyMessage":"","validationText":"{cutoff.strftime("%Y-%m-%d")}-00-00-00","valueAsString":"{cutoff.strftime("%Y-%m-%d")}-00-00-00","minDateStr":"1900-01-01-00-00-00","maxDateStr":"9999-12-31-00-00-00","lastSetTextBoxValue":"{start_str}"}}',
            "cphNoMargin_f_ddcDateFiledTo_clientState": f'{{"enabled":true,"emptyMessage":"","validationText":"{now.strftime("%Y-%m-%d")}-00-00-00","valueAsString":"{now.strftime("%Y-%m-%d")}-00-00-00","minDateStr":"1900-01-01-00-00-00","maxDateStr":"9999-12-31-00-00-00","lastSetTextBoxValue":"{end_str}"}}',
            "ctl00$cphNoMargin$SearchButtons1$btnSearch": "Search",
        }
        # Add all distress checkboxes
        for idx in DOC_TYPES:
            form[f"ctl00$cphNoMargin$f$dclDocType${idx}"] = "on"

        r = client.post(SEARCH_URL, data=form,
                       headers={**HEADERS, "Referer": SEARCH_URL, "Content-Type": "application/x-www-form-urlencoded"})
        log.info("[Travis] Search POST: %s url=%s", r.status_code, r.url)

        for page_num in range(1, MAX_PAGES + 1):
            soup = BeautifulSoup(r.text, "lxml")
            recs = parse_results(soup)
            log.info("[Travis] Page %d: %d records", page_num, len(recs))
            if page_num == 1: log.info("[Travis] HTML sample: %s", r.text[3000:3500])
            all_records.extend(recs)
            if not recs: break
            next_link = soup.find("a", string=re.compile(r"Next", re.I))
            if not next_link: break
            vs2 = get_vs(soup)
            href = next_link.get("href","")
            target = re.search(r"'([^']+)'", href)
            r = client.post(RESULTS_URL, data={**vs2, "__EVENTTARGET": target.group(1) if target else "", "__EVENTARGUMENT": ""},
                           headers={**HEADERS, "Referer": RESULTS_URL, "Content-Type": "application/x-www-form-urlencoded"})

    seen, deduped = set(), []
    for rec in all_records:
        k = rec.get("doc_num","")
        if k and k not in seen:
            seen.add(k); deduped.append(rec)

    log.info("[Travis] %d unique records", len(deduped))
    out_dir = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "travis_records.json"), "w") as f:
        json.dump({
            "fetched_at": datetime.now().isoformat(),
            "source": "Travis County Clerk (tccsearch.org)",
            "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
            "total": len(deduped), "counties": [COUNTY], "records": deduped
        }, f, indent=2, default=str)
    log.info("[Travis] Done")
    return deduped

if __name__ == "__main__":
    scrape()
