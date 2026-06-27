import os, re, json, logging, httpx
from datetime import datetime, timedelta
from urllib.parse import urlencode
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("travis")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "10"))
COUNTY        = "travis"
BASE          = "https://www.tccsearch.org"
SEARCH_URL    = f"{BASE}/RealEstate/SearchEntry.aspx"
RESULTS_URL   = f"{BASE}/RealEstate/SearchResults.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

PROXY_USER = os.getenv("DECODO_USER", "")
PROXY_PASS = os.getenv("DECODO_PASS", "")
PROXY_URL  = f"http://{PROXY_USER}:{PROXY_PASS}@state.decodo.com:14001" if PROXY_USER else None

DOC_TYPES = [
    "LIS PENDENS", "ABSTRACT OF JUDGMENT", "TAX DEED",
    "FEDERAL TAX LIEN", "MECHANIC LIEN", "PROBATE",
    "STATE TAX LIEN", "NOTICE OF FORECLOSURE",
]

def cat_from_doc_type(dt):
    dt = dt.upper()
    if "LIS PEND" in dt:    return ("LP",      "Lis Pendens")
    if "FED TAX" in dt:     return ("LNFED",   "Federal Tax Lien")
    if "ST TAX" in dt or "STATE TAX" in dt: return ("LNSTATE", "State Tax Lien")
    if "ABSTRACT" in dt or "JUDGMENT" in dt: return ("JUD",   "Abstract of Judgment")
    if "PROB" in dt:        return ("PRO",     "Probate")
    if "TAX DEED" in dt:    return ("TAXDEED", "Tax Deed")
    if "MECHANIC" in dt:    return ("LNMECH",  "Mechanic Lien")
    if "FORECLOSURE" in dt: return ("NOFC",    "Notice of Foreclosure")
    if "LIEN" in dt:        return ("LN",      "Lien")
    return ("LN", dt)

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def get_hidden(soup):
    out = {}
    for fld in ["__VIEWSTATE","__EVENTVALIDATION","__VIEWSTATEGENERATOR","__EVENTTARGET","__EVENTARGUMENT"]:
        el = soup.find("input", {"id": fld}) or soup.find("input", {"name": fld})
        out[fld] = el["value"] if el and el.get("value") else ""
    return out

def parse_results(soup):
    records = []
    table = soup.find("table", {"class": re.compile(r"result", re.I)}) or soup.find("table")
    if not table: return records
    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5: continue
        texts = [c.get_text(" ", strip=True) for c in cells]
        # Col layout: # | Image | checkbox | Instrument# | Date Filed | Doc Type | Name/Associated | Legal | Status
        doc_num  = texts[3].strip() if len(texts) > 3 else ""
        filed    = norm_date(texts[4]) if len(texts) > 4 else ""
        doc_type = texts[5].strip() if len(texts) > 5 else ""
        name_col = texts[6] if len(texts) > 6 else ""
        legal    = texts[7].strip() if len(texts) > 7 else ""

        if not doc_num or not re.match(r'\d{4}', doc_num): continue

        # Parse [R] grantor and [E] grantee from name column
        grantor = ""
        grantee = ""
        r_match = re.search(r'\[R\]\s*([^\[]+)', name_col)
        e_match = re.search(r'\[E\]\s*([^\[]+)', name_col)
        if r_match: grantor = re.sub(r'\s+', ' ', r_match.group(1)).strip()
        if e_match: grantee = re.sub(r'\s+', ' ', e_match.group(1)).strip()

        # For distress docs owner is the defendant [E]
        owner = grantee if grantee else grantor

        if not owner or len(owner) < 3: continue
        cat, cat_label = cat_from_doc_type(doc_type)
        records.append({
            "doc_num": doc_num, "doc_type": doc_type,
            "cat": cat, "cat_label": cat_label,
            "filed": filed, "owner": owner, "grantee": grantee,
            "amount": None, "legal": legal, "county": COUNTY,
            "clerk_url": RESULTS_URL,
            "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
            "score": 0, "flags": [],
        })
    return records

def scrape():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start_str = cutoff.strftime("%m/%d/%Y")
    end_str   = now.strftime("%m/%d/%Y")
    log.info("[Travis] Scraping %s to %s", start_str, end_str)
    all_records = []
    log.info("[Travis] PROXY_USER=%s PROXY_URL=%s", PROXY_USER, PROXY_URL)

    client_kwargs = {"headers": HEADERS, "follow_redirects": True, "timeout": 30}
    if PROXY_URL:
        client_kwargs["proxy"] = PROXY_URL
    with httpx.Client(**client_kwargs) as client:
        # Step 1 — hit homepage to get session cookie
        r = client.get(BASE)
        log.info("[Travis] Homepage: %s", r.status_code)

        # Step 2 — click disclaimer
        soup = BeautifulSoup(r.text, "lxml")
        disc_link = soup.find("a", string=re.compile(r"Click here", re.I))
        if disc_link and disc_link.get("href"):
            href = disc_link["href"]
            if not href.startswith("http"): href = BASE + href
            r = client.get(href)
            log.info("[Travis] Disclaimer clicked: %s", r.status_code)

        # Step 3 — load search page
        r = client.get(SEARCH_URL)
        log.info("[Travis] Search page: %s", r.status_code)
        soup = BeautifulSoup(r.text, "lxml")
        hidden = get_hidden(soup)
        if not hidden.get("__VIEWSTATE"):
            log.error("[Travis] No VIEWSTATE — blocked")
            return []

        # Step 4 — find doc type checkbox names
        checkboxes = soup.find_all("input", {"type": "checkbox"})
        log.info("[Travis] Found %d checkboxes", len(checkboxes))

        # Build checkbox map: label text -> input name
        cb_map = {}
        for cb in checkboxes:
            name = cb.get("name","")
            # Find label
            label = cb.find_next("label") or cb.find_next(string=True)
            text = cb.get_attribute_list("value")[0] if cb.get("value") else ""
            parent_text = cb.parent.get_text(" ", strip=True) if cb.parent else ""
            cb_map[parent_text.upper()] = name

        log.info("[Travis] Checkbox map sample: %s", list(cb_map.items())[:5])

        for doc_type in DOC_TYPES:
            # Find matching checkbox
            cb_name = None
            for label, name in cb_map.items():
                if doc_type.upper() in label:
                    cb_name = name
                    break

            if not cb_name:
                log.warning("[Travis] No checkbox found for %s", doc_type)
                continue

            log.info("[Travis] Searching %s via %s", doc_type, cb_name)

            # POST search form
            form_data = {
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                **hidden,
                "cphNoMargin_f_txtDateFiledFrom": start_str,
                "cphNoMargin_f_txtDateFiledTo": end_str,
                cb_name: "on",
                "cphNoMargin_SearchButtons1_btnSearch": "Search",
            }

            r = client.post(SEARCH_URL, data=form_data,
                           headers={**HEADERS, "Referer": SEARCH_URL,
                                    "Content-Type": "application/x-www-form-urlencoded"})
            log.info("[Travis] %s POST: %s", doc_type, r.status_code)

            for page_num in range(1, MAX_PAGES + 1):
                soup = BeautifulSoup(r.text, "lxml")
                recs = parse_results(soup)
                log.info("[Travis] %s page %d: %d records", doc_type, page_num, len(recs))
                all_records.extend(recs)
                if not recs: break

                # Next page
                next_btn = soup.find("input", {"value": re.compile(r"next|>", re.I)})
                if not next_btn: break
                hidden2 = get_hidden(soup)
                page_data = {**hidden2, "__EVENTTARGET": next_btn.get("name",""), "__EVENTARGUMENT": ""}
                r = client.post(RESULTS_URL, data=page_data,
                               headers={**HEADERS, "Referer": RESULTS_URL,
                                        "Content-Type": "application/x-www-form-urlencoded"})

    seen, deduped = set(), []
    for rec in all_records:
        k = rec.get("doc_num","")
        if k and k not in seen:
            seen.add(k); deduped.append(rec)

    log.info("[Travis] %d unique records total", len(deduped))
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






