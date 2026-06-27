import os, re, json, logging, httpx
from datetime import datetime, timedelta
from urllib.parse import quote
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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
}

# Checkbox index -> value (from form inspection)
DOC_TYPES = {
    63: "LIS PEND",
    1:  "AJ",
    51: "FED TAX",
    65: "ML",
    25: "PROB",
    107:"ST TAX LIEN",
    72: "FORECLOSURE",
    24: "JUDGMT",
    60: "JDGMT",
    62: "LIEN",
}

def cat_from_val(val):
    v = val.upper()
    if "LIS" in v:       return ("LP",      "Lis Pendens")
    if "FED" in v:       return ("LNFED",   "Federal Tax Lien")
    if "ST TAX" in v:    return ("LNSTATE", "State Tax Lien")
    if v in ("AJ","JUDGMT","JDGMT"): return ("JUD", "Abstract of Judgment")
    if "PROB" in v:      return ("PRO",     "Probate")
    if "ML" == v:        return ("LNMECH",  "Mechanic Lien")
    if "FORE" in v:      return ("NOFC",    "Notice of Foreclosure")
    return ("LN", "Lien")

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

def make_date_cs(dt):
    return f"|0|01{dt.year}-{dt.month}-{dt.day}-0-0-0-0||[[[[]]%2C[]%2C[]]%2C[{{}}%2C[]]%2C\"01{dt.year}-{dt.month}-{dt.day}-0-0-0-0\"]"

def parse_results(html):
    records = []
    soup = BeautifulSoup(html, "lxml")
    # Find results table - look for table with instrument numbers
    for t in soup.find_all("table"):
        rows = t.find_all("tr")
        found_data = False
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5: continue
            texts = [c.get_text(" ", strip=True) for c in cells]
            # Instrument number is typically 2026XXXXXX format
            doc_num = ""
            for txt in texts:
                if re.match(r'^\d{10}$', txt.strip()):
                    doc_num = txt.strip()
                    break
            if not doc_num: continue
            found_data = True
            # Find date filed
            filed = ""
            for txt in texts:
                if re.match(r'\d{2}/\d{2}/\d{4}', txt.strip()):
                    filed = norm_date(txt.strip())
                    break
            # Find doc type
            doc_type = ""
            for txt in texts:
                if any(k in txt.upper() for k in ["LIS PEND","JUDGMENT","TAX LIEN","MECHANIC","PROBATE","FORECLOSURE","LIEN"]):
                    doc_type = txt.strip()
                    break
            # Parse names
            name_col = " ".join(texts)
            r_match = re.search(r'\[R\]\s*([^\[]+)', name_col)
            e_match = re.search(r'\[E\]\s*([^\[]+)', name_col)
            grantor = re.sub(r'\s+', ' ', r_match.group(1)).strip() if r_match else ""
            grantee = re.sub(r'\s+', ' ', e_match.group(1)).strip() if e_match else ""
            owner   = grantee if grantee else grantor
            if not owner or len(owner) < 3: continue
            cat, cat_label = cat_from_val(doc_type)
            records.append({
                "doc_num": doc_num, "doc_type": doc_type,
                "cat": cat, "cat_label": cat_label,
                "filed": filed, "owner": owner, "grantee": grantee,
                "amount": None, "legal": "", "county": COUNTY,
                "clerk_url": RESULTS_URL,
                "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                "score": 0, "flags": [],
            })
        if found_data: break
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
        log.info("[Travis] Search page: %s", r.status_code)

        # Get all clientState values from page
        def get_cs(name):
            el = soup.find("input", {"name": name})
            return el.get("value","") if el else ""

        # Build form with exact clientState format from browser capture
        from_cs = f"|0|01{cutoff.year}-{cutoff.month}-{cutoff.day}-0-0-0-0||[[[[]]%2C[]%2C[]]%2C[{{}}%2C[]]%2C\"01{cutoff.year}-{cutoff.month}-{cutoff.day}-0-0-0-0\"]"
        to_cs   = f"|0|01{now.year}-{now.month}-{now.day}-0-0-0-0||[[[[]]%2C[]%2C[]]%2C[{{}}%2C[]]%2C\"01{now.year}-{now.month}-{now.day}-0-0-0-0\"]"

        base_form = {
            **vs,
            "__EVENTTARGET": "ctl00$cphNoMargin$SearchButtons2$btnSearch",
            "__EVENTARGUMENT": "0",
            "Header1_WebHDS_clientState": "",
            "Header1_WebDataMenu1_clientState": get_cs("Header1_WebDataMenu1_clientState"),
            "ctl00$cphNoMargin$f$NameSearchMode": "rdoCombine",
            "cphNoMargin_f_txtParty_clientState": get_cs("cphNoMargin_f_txtParty_clientState"),
            "cphNoMargin_f_txtParty": "",
            "ctl00$cphNoMargin$f$drbPartyType": "",
            "cphNoMargin_f_txtGrantor_clientState": get_cs("cphNoMargin_f_txtGrantor_clientState"),
            "cphNoMargin_f_txtGrantee_clientState": get_cs("cphNoMargin_f_txtGrantee_clientState"),
            "cphNoMargin_f_ddcDateFiledFrom_clientState": from_cs,
            "cphNoMargin_f_ddcDateFiledTo_clientState": to_cs,
            "cphNoMargin_f_txtInstrumentNoFrom_clientState": get_cs("cphNoMargin_f_txtInstrumentNoFrom_clientState"),
            "cphNoMargin_f_txtInstrumentNoFrom": "",
            "cphNoMargin_f_txtInstrumentNoTo_clientState": get_cs("cphNoMargin_f_txtInstrumentNoTo_clientState"),
            "cphNoMargin_f_txtInstrumentNoTo": "",
            "cphNoMargin_f_txtBook_clientState": get_cs("cphNoMargin_f_txtBook_clientState"),
            "cphNoMargin_f_txtBook": "",
            "cphNoMargin_f_txtPage_clientState": get_cs("cphNoMargin_f_txtPage_clientState"),
            "cphNoMargin_f_txtPage": "",
            "cphNoMargin_f_DataTextEdit1_clientState": get_cs("cphNoMargin_f_DataTextEdit1_clientState"),
            "cphNoMargin_f_DataTextEdit1": "",
            "cphNoMargin_f_txtLDStreetAddress_clientState": get_cs("cphNoMargin_f_txtLDStreetAddress_clientState"),
            "cphNoMargin_f_txtLDStreetAddress": "",
            "cphNoMargin_f_txtLDLot_clientState": get_cs("cphNoMargin_f_txtLDLot_clientState"),
            "cphNoMargin_f_txtLDLot": "",
            "cphNoMargin_f_txtLDBook_clientState": get_cs("cphNoMargin_f_txtLDBook_clientState"),
            "cphNoMargin_f_txtLDBook": "",
            "cphNoMargin_f_txtLDSection_clientState": get_cs("cphNoMargin_f_txtLDSection_clientState"),
            "cphNoMargin_f_txtLDSection": "",
            "cphNoMargin_f_txtLDVolume_clientState": get_cs("cphNoMargin_f_txtLDVolume_clientState"),
            "cphNoMargin_f_txtLDVolume": "",
            "cphNoMargin_f_txtLDFreeForm_clientState": get_cs("cphNoMargin_f_txtLDFreeForm_clientState"),
            "cphNoMargin_f_txtLDFreeForm": "",
            "cphNoMargin_dlgPopup_clientState": get_cs("cphNoMargin_dlgPopup_clientState"),
            "dlgOptionWindow_clientState": get_cs("dlgOptionWindow_clientState"),
            "RangeContextMenu_clientState": get_cs("RangeContextMenu_clientState"),
            "LoginForm1_txtLogonName_clientState": get_cs("LoginForm1_txtLogonName_clientState"),
            "LoginForm1_txtLogonName": "",
            "LoginForm1_txtPassword_clientState": get_cs("LoginForm1_txtPassword_clientState"),
            "LoginForm1_txtPassword": "",
            "ctl00$LoginForm1$logonType": "rdoPubCpu",
            "_ig_def_dp_cal_clientState": get_cs("_ig_def_dp_cal_clientState"),
            "ctl00$cphNoMargin$SearchButtons2$btnSearch__10": ":0",
        }

        # Add all doc type checkboxes
        for idx, val in DOC_TYPES.items():
            base_form[f"ctl00$cphNoMargin$f$dclDocType${idx}"] = val

        r = client.post(SEARCH_URL, data=base_form,
                       headers={**HEADERS, "Referer": SEARCH_URL,
                                "Content-Type": "application/x-www-form-urlencoded"})
        log.info("[Travis] Search POST: %s url=%s", r.status_code, r.url)

        for page_num in range(1, MAX_PAGES + 1):
            recs = parse_results(r.text)
            log.info("[Travis] Page %d: %d records (url=%s)", page_num, len(recs), r.url)
            all_records.extend(recs)
            if not recs: break
            soup2 = BeautifulSoup(r.text, "lxml")
            next_link = soup2.find("a", string=re.compile(r"Next", re.I))
            if not next_link: break
            vs2 = get_vs(soup2)
            href = next_link.get("href","")
            target = re.search(r"'([^']+)'", href)
            r = client.post(RESULTS_URL, data={**vs2,
                "__EVENTTARGET": target.group(1) if target else "",
                "__EVENTARGUMENT": ""},
                headers={**HEADERS, "Referer": RESULTS_URL,
                         "Content-Type": "application/x-www-form-urlencoded"})

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
