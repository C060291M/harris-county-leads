import os, re, json, logging, httpx
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("fortbend")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "10"))
COUNTY        = "fort bend"
BASE          = "https://ccweb.co.fort-bend.tx.us"
SEARCH_URL    = f"{BASE}/RealEstate/SearchEntry.aspx"
RESULTS_URL   = f"{BASE}/RealEstate/SearchResults.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
}

DOC_TYPES = {
    "LIS PENDENS": "LP",
    "ABSTRACT OF JUDGMENT": "JUD",
    "FEDERAL TAX LIEN": "LNFED",
    "MECHANIC LIEN": "LNMECH",
    "STATE TAX LIEN": "LNSTATE",
    "PROBATE": "PRO",
    "JUDGMENT": "JUD",
}

def make_cs(dt):
    s = "01%d-%d-%d-0-0-0-0" % (dt.year, dt.month, dt.day)
    return "|0|" + s + "||[[[[]],[],[]]," + '[{},[]],"' + s + '"]'

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

def parse_results(html):
    records = []
    soup = BeautifulSoup(html, "lxml")
    for t in soup.find_all("table"):
        rows = t.find_all("tr")
        found = False
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5: continue
            texts = [c.get_text(" ", strip=True) for c in cells]
            doc_num = ""
            for txt in texts:
                if re.match(r'^\d{10}$', txt.strip()):
                    doc_num = txt.strip()
                    break
            if not doc_num: continue
            found = True
            filed = ""
            for txt in texts:
                if re.match(r'\d{2}/\d{2}/\d{4}', txt.strip()):
                    filed = norm_date(txt.strip())
                    break
            doc_type = ""
            for txt in texts:
                if any(k in txt.upper() for k in ["LIS PEND","JUDGMENT","TAX LIEN","MECHANIC","PROBATE","FORECLOSURE"]):
                    doc_type = txt.strip()
                    break
            name_col = " ".join(texts)
            r_match = re.search(r'\[R\]\s*([^\[]+)', name_col)
            e_match = re.search(r'\[E\]\s*([^\[]+)', name_col)
            grantor = re.sub(r'\s+', ' ', r_match.group(1)).strip() if r_match else ""
            grantee = re.sub(r'\s+', ' ', e_match.group(1)).strip() if e_match else ""
            owner = grantee if grantee else grantor
            if not owner or len(owner) < 3: continue
            dt = doc_type.upper()
            if "LIS" in dt: cat,lbl = "LP","Lis Pendens"
            elif "FED" in dt: cat,lbl = "LNFED","Federal Tax Lien"
            elif "ST TAX" in dt or "STATE" in dt: cat,lbl = "LNSTATE","State Tax Lien"
            elif "MECH" in dt: cat,lbl = "LNMECH","Mechanic Lien"
            elif "PROB" in dt: cat,lbl = "PRO","Probate"
            elif "JUDG" in dt: cat,lbl = "JUD","Abstract of Judgment"
            else: cat,lbl = "LN","Lien"
            records.append({
                "doc_num": doc_num, "doc_type": doc_type,
                "cat": cat, "cat_label": lbl,
                "filed": filed, "owner": owner, "grantee": grantee,
                "amount": None, "legal": "", "county": COUNTY,
                "clerk_url": RESULTS_URL,
                "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                "score": 0, "flags": [],
            })
        if found: break
    return records

def scrape():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("[Fort Bend] Scraping %s to %s", cutoff.strftime("%m/%d/%Y"), now.strftime("%m/%d/%Y"))

    all_records = []
    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30) as client:
        # Disclaimer
        r = client.get(BASE)
        soup = BeautifulSoup(r.text, "lxml")
        vs = get_vs(soup)
        r = client.post(BASE, data={**vs, "__EVENTTARGET": "ctl00$cph1$lnkAccept", "__EVENTARGUMENT": ""},
                       headers={**HEADERS, "Referer": BASE, "Content-Type": "application/x-www-form-urlencoded"})
        log.info("[Fort Bend] Disclaimer: %s", r.status_code)

        # Search page
        r = client.get(SEARCH_URL)
        soup = BeautifulSoup(r.text, "lxml")
        vs = get_vs(soup)

        def get_cs(name):
            el = soup.find("input", {"name": name})
            return el.get("value","") if el else ""

        # Search all doc types at once
        form = {
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
            "cphNoMargin_f_ddcDateFiledFrom_clientState": make_cs(cutoff),
            "cphNoMargin_f_ddcDateFiledTo_clientState": make_cs(now),
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

        # Get checkboxes for our doc types
        for inp in soup.find_all("input", {"type": "checkbox"}):
            val = inp.get("value","").upper()
            name = inp.get("name","")
            if any(k in val for k in ["LIS","ABSTRACT","FEDERAL","MECHANIC","STATE TAX","PROBATE","JUDGMENT"]):
                form[name] = inp.get("value","")

        r = client.post(SEARCH_URL, data=form,
                       headers={**HEADERS, "Referer": SEARCH_URL, "Content-Type": "application/x-www-form-urlencoded"})
        log.info("[Fort Bend] Search: %s url=%s", r.status_code, r.url)

        for page_num in range(1, MAX_PAGES + 1):
            recs = parse_results(r.text)
            log.info("[Fort Bend] Page %d: %d records", page_num, len(recs))
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
                headers={**HEADERS, "Referer": RESULTS_URL, "Content-Type": "application/x-www-form-urlencoded"})

    seen, deduped = set(), []
    for rec in all_records:
        k = rec.get("doc_num","")
        if k and k not in seen:
            seen.add(k); deduped.append(rec)

    log.info("[Fort Bend] %d unique records", len(deduped))
    out_dir = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "fort_bend_records.json"), "w") as f:
        json.dump({
            "fetched_at": datetime.now().isoformat(),
            "source": "Fort Bend County Clerk",
            "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
            "total": len(deduped), "counties": [COUNTY], "records": deduped
        }, f, indent=2, default=str)
    log.info("[Fort Bend] Done")
    return deduped

if __name__ == "__main__":
    scrape()
