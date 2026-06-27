import os, re, json, logging, httpx
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("travis")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))
COUNTY        = "travis"
BASE_URL      = "https://www.tccsearch.org/RealEstate/SearchEntry.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

DISTRESS_DOC_TYPES = {
    1:"AJ", 23:"DIVOR", 24:"JUDGMT", 25:"PROB", 51:"FED TAX",
    60:"JDGMT", 62:"LIEN", 63:"LIS PEND", 65:"ML",
    72:"FORECLOSURE", 107:"ST TAX LIEN", 56:"HOSP LIEN",
}

def cat_from_doc_type(dt):
    dt = dt.upper()
    if "LIS PEND" in dt:    return ("LP",      "Lis Pendens")
    if "FED TAX" in dt:     return ("LNFED",   "Federal Tax Lien")
    if "ST TAX" in dt:      return ("LNSTATE", "State Tax Lien")
    if "JUDGMT" in dt or "JDGMT" in dt or dt=="AJ": return ("JUD","Abstract of Judgment")
    if "PROB" in dt:        return ("PRO",     "Probate")
    if "DIVOR" in dt:       return ("DIV",     "Divorce")
    if dt == "ML":          return ("LNMECH",  "Mechanic Lien")
    if "FORECLOSURE" in dt: return ("NOFC",    "Notice of Foreclosure")
    if "LIEN" in dt:        return ("LN",      "Lien")
    return ("LN", dt)

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def parse_name(raw):
    if not raw: return ""
    raw = re.sub(r'\[.\]', '', raw).strip()
    raw = re.sub(r'\(\+\)', '', raw).strip()
    return re.sub(r'\s+', ' ', raw).strip()

def parse_table(soup):
    records = []
    for t in soup.find_all("table"):
        rows = t.find_all("tr")
        if len(rows) < 5: continue
        for row in rows[:3]:
            cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
            if cells and re.match(r'^\d+$', cells[0].strip()) and len(cells) > 8:
                for row2 in rows:
                    cells2 = [c.get_text(" ", strip=True) for c in row2.find_all("td")]
                    if not cells2 or not re.match(r'^\d+$', cells2[0].strip()): continue
                    if len(cells2) < 20: continue
                    doc_num  = cells2[3].strip()  if len(cells2) > 3  else ""
                    filed    = norm_date(cells2[8]) if len(cells2) > 8  else ""
                    doc_type = cells2[9].strip()  if len(cells2) > 9  else ""
                    grantor  = parse_name(cells2[14]) if len(cells2) > 14 else parse_name(cells2[11] if len(cells2) > 11 else "")
                    grantee  = parse_name(cells2[18]) if len(cells2) > 18 else ""
                    legal    = cells2[20].strip() if len(cells2) > 20 else ""
                    if not doc_num: continue
                    cat, cat_label = cat_from_doc_type(doc_type)
                    records.append({
                        "doc_num": doc_num, "doc_type": doc_type,
                        "cat": cat, "cat_label": cat_label,
                        "filed": filed, "owner": grantor, "grantee": grantee,
                        "amount": None, "county": COUNTY, "legal": legal,
                        "clerk_url": BASE_URL,
                        "prop_address":"","score":0,"flags":[],
                    })
                return records
    return records

def get_hidden(soup):
    vs  = soup.find("input", {"id": "__VIEWSTATE"})
    evv = soup.find("input", {"id": "__EVENTVALIDATION"})
    vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
    return {
        "__VIEWSTATE":          vs["value"]  if vs  else "",
        "__EVENTVALIDATION":    evv["value"] if evv else "",
        "__VIEWSTATEGENERATOR": vsg["value"] if vsg else "",
    }

def scrape():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start_str = cutoff.strftime("%m/%d/%Y")
    end_str   = now.strftime("%m/%d/%Y")
    log.info("[Travis] Scraping %s to %s", start_str, end_str)
    records = []

    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30) as client:
        # Step 1 — load homepage to get session cookie
        r = client.get("https://www.tccsearch.org")
        log.info("[Travis] Homepage: %s", r.status_code)

        # Step 2 — load search page to get VIEWSTATE
        r = client.get(BASE_URL)
        log.info("[Travis] Search page: %s", r.status_code)
        soup = BeautifulSoup(r.text, "lxml")
        hidden = get_hidden(soup)
        log.info("[Travis] VIEWSTATE len=%d", len(hidden["__VIEWSTATE"]))

        if not hidden["__VIEWSTATE"]:
            log.error("[Travis] No VIEWSTATE found — page may be blocked")
            return []

        # Step 3 — build form POST
        form_data = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            **hidden,
            "cphNoMargin_f_ddcDateFiledFrom$TextBox1": start_str,
            "cphNoMargin_f_ddcDateFiledTo$TextBox1":   end_str,
            "cphNoMargin_SearchButtons1_btnSearch":    "Search",
        }
        # Add doc type checkboxes
        for idx in DISTRESS_DOC_TYPES:
            form_data[f"cphNoMargin_f_dclDocType_{idx}"] = "on"

        r = client.post(BASE_URL, data=form_data,
                        headers={**HEADERS, "Referer": BASE_URL,
                                 "Content-Type": "application/x-www-form-urlencoded"})
        log.info("[Travis] Search POST: %s url=%s", r.status_code, r.url)

        for page_num in range(1, MAX_PAGES + 1):
            soup = BeautifulSoup(r.text, "lxml")
            page_recs = parse_table(soup)
            log.info("[Travis] Page %d: %d records", page_num, len(page_recs))
            records.extend(page_recs)
            if not page_recs: break

            # Pagination — click Next via __EVENTTARGET
            next_link = soup.find("a", string=re.compile(r"Next", re.I))
            if not next_link: break
            hidden = get_hidden(soup)
            page_data = {
                "__EVENTTARGET":   next_link.get("href","").replace("javascript:__doPostBack('","").split("'")[0],
                "__EVENTARGUMENT": "",
                **hidden,
            }
            r = client.post(BASE_URL, data=page_data,
                            headers={**HEADERS, "Referer": BASE_URL,
                                     "Content-Type": "application/x-www-form-urlencoded"})

    seen, deduped = set(), []
    for rec in records:
        k = rec.get("doc_num","")
        if k and k not in seen:
            seen.add(k); deduped.append(rec)

    log.info("[Travis] %d unique records", len(deduped))

    out_dir  = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "travis_records.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({
            "fetched_at": datetime.now().isoformat(),
            "source": "Travis County Clerk (tccsearch.org)",
            "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
            "total": len(deduped), "counties": [COUNTY], "records": deduped
        }, f, indent=2, default=str)
    log.info("[Travis] Written to %s", out_file)
    return deduped

if __name__ == "__main__":
    scrape()
