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

DOC_TYPES = [
    "LIS PENDENS", "ABSTRACT OF JUDGMENT", "TAX DEED",
    "FEDERAL TAX LIEN", "MECHANIC LIEN", "PROBATE",
    "STATE TAX LIEN", "NOTICE OF FORECLOSURE",
]

def cat_from_doc_type(dt):
    dt = dt.upper()
    if "LIS PEND" in dt:     return ("LP",      "Lis Pendens")
    if "FED TAX" in dt:      return ("LNFED",   "Federal Tax Lien")
    if "STATE TAX" in dt:    return ("LNSTATE", "State Tax Lien")
    if "ABSTRACT" in dt:     return ("JUD",     "Abstract of Judgment")
    if "PROB" in dt:         return ("PRO",     "Probate")
    if "TAX DEED" in dt:     return ("TAXDEED", "Tax Deed")
    if "MECHANIC" in dt:     return ("LNMECH",  "Mechanic Lien")
    if "FORECLOSURE" in dt:  return ("NOFC",    "Notice of Foreclosure")
    return ("LN", dt)

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def get_viewstate(soup):
    out = {}
    for fld in ["__VIEWSTATE","__EVENTVALIDATION","__VIEWSTATEGENERATOR"]:
        el = soup.find("input", {"name": fld})
        out[fld] = el.get("value","") if el else ""
    return out

def parse_results(soup):
    records = []
    for t in soup.find_all("table"):
        rows = t.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5: continue
            texts = [c.get_text(" ", strip=True) for c in cells]
            doc_num  = texts[3].strip() if len(texts) > 3 else ""
            if not doc_num or not re.match(r'\d{4}', doc_num): continue
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
        if records: break
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
        # Step 1 — homepage
        r = client.get(BASE)
        log.info("[Travis] Homepage: %s", r.status_code)
        if r.status_code != 200:
            log.error("[Travis] Homepage blocked")
            return []

        # Step 2 — disclaimer POST
        soup = BeautifulSoup(r.text, "lxml")
        vs = get_viewstate(soup)
        r = client.post(BASE, data={
            **vs,
            "__EVENTTARGET": "ctl00$cph1$lnkAccept",
            "__EVENTARGUMENT": "",
        }, headers={**HEADERS, "Referer": BASE, "Content-Type": "application/x-www-form-urlencoded"})
        log.info("[Travis] Disclaimer: %s", r.status_code)

        # Step 3 — load search page
        r = client.get(SEARCH_URL)
        log.info("[Travis] Search page: %s", r.status_code)
        soup = BeautifulSoup(r.text, "lxml")
        vs = get_viewstate(soup)
        if not vs.get("__VIEWSTATE"):
            log.error("[Travis] No VIEWSTATE on search page")
            return []

        # Step 4 — find doc type checkboxes
        cb_map = {}
        for cb in soup.find_all("input", {"type": "checkbox"}):
            name = cb.get("name","")
            label = cb.find_next_sibling(string=True) or ""
            parent = cb.parent.get_text(" ", strip=True).upper() if cb.parent else ""
            cb_map[parent] = name

        log.info("[Travis] Found %d checkboxes", len(cb_map))
        for lbl, nm in list(cb_map.items())[:20]:
            log.info("[Travis] CB: %r -> %s", lbl[:50], nm)

        # Step 5 — search each doc type
        for doc_type in DOC_TYPES:
            cb_name = None
            for label, name in cb_map.items():
                if doc_type in label:
                    cb_name = name
                    break
            if not cb_name:
                log.warning("[Travis] No checkbox for %s", doc_type)
                continue

            form = {
                **vs,
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "cphNoMargin_f_txtDateFiledFrom": start_str,
                "cphNoMargin_f_txtDateFiledTo": end_str,
                cb_name: "on",
                "cphNoMargin_SearchButtons1_btnSearch": "Search",
            }
            r = client.post(SEARCH_URL, data=form,
                           headers={**HEADERS, "Referer": SEARCH_URL,
                                    "Content-Type": "application/x-www-form-urlencoded"})
            log.info("[Travis] %s search: %s", doc_type, r.status_code)

            for page_num in range(1, MAX_PAGES + 1):
                soup = BeautifulSoup(r.text, "lxml")
                recs = parse_results(soup)
                log.info("[Travis] %s page %d: %d records", doc_type, page_num, len(recs))
            if page_num == 1 and not recs:
                log.info("[Travis] Sample HTML: %s", str(soup)[:500])
                all_records.extend(recs)
                if not recs: break
                next_btn = soup.find("a", string=re.compile(r"Next", re.I))
                if not next_btn: break
                vs2 = get_viewstate(soup)
                href = next_btn.get("href","")
                target = re.search(r"'([^']+)'", href)
                r = client.post(RESULTS_URL, data={
                    **vs2,
                    "__EVENTTARGET": target.group(1) if target else "",
                    "__EVENTARGUMENT": "",
                }, headers={**HEADERS, "Referer": RESULTS_URL,
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


