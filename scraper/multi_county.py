"""
StackIQ Multi-County Texas Scraper
All 5 counties use publicsearch.us — same platform, same API.
"""
import json, logging, re, time, os
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("multi_county")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))
API_URL = os.getenv("API_URL", "https://api.stackiq.org/leads/bulk-import")

COUNTIES = {
    "Dallas":  "dallas.tx.publicsearch.us",
    "Tarrant": "tarrant.tx.publicsearch.us",
    "Bexar":   "bexar.tx.publicsearch.us",
    "Travis":  "travis.tx.publicsearch.us",
    "Collin":  "collin.tx.publicsearch.us",
}

DOC_TYPE_MAP = {
    "LP":   ("LP",      "Lis Pendens"),
    "LMH":  ("LNMECH",  "Mechanic Lien"),
    "LH":   ("LNHOA",   "HOA Lien"),
    "LF":   ("LNFED",   "Federal Tax Lien"),
    "LIS":  ("LNIRS",   "IRS Lien"),
    "LST":  ("LNSTATE", "State Tax Lien"),
    "JA":   ("JUD",     "Abstract of Judgment"),
    "NF":   ("NOFC",    "Notice of Foreclosure"),
    "TD":   ("TAXDEED", "Tax Deed"),
    "PRO":  ("PRO",     "Probate"),
    "DIV":  ("DIV",     "Divorce"),
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%Y/%m/%d","%d/%m/%Y"):
        try: return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()

def parse_amount(raw):
    if not raw: return None
    cleaned = re.sub(r"[^\d.]", "", str(raw))
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except: return None

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
            if datetime.strptime(filed_str,"%Y-%m-%d") >= cutoff: flags.append("New this week"); s += 5
        except: pass
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
        "score": 0, "flags": [],
    }

def scrape_county(name, host, start_dt, end_dt):
    log.info("%s - scraping %s to %s", name, start_dt.strftime("%m/%d/%Y"), end_dt.strftime("%m/%d/%Y"))
    records = []
    base = f"https://{host}"
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{base}/",
        "Origin": base,
    })

    # PublicSearch.us API endpoint
    api = f"{base}/api/instruments/search/advanced"

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str   = end_dt.strftime("%Y-%m-%d")

    for code, (cat, cat_label) in DOC_TYPE_MAP.items():
        try:
            payload = {
                "docTypes": [code],
                "dateField": "RecordedDate",
                "startDate": start_str,
                "endDate":   end_str,
                "offset": 0,
                "limit":  200,
                "sort":   "desc",
            }
            r = session.post(api, json=payload, timeout=30)
            if r.status_code != 200:
                # Try alternate endpoint
                api2 = f"{base}/api/instruments/search"
                payload2 = {
                    "docTypeCode": code,
                    "startDate": start_str,
                    "endDate": end_str,
                    "offset": 0,
                    "limit": 200,
                }
                r = session.post(api2, json=payload2, timeout=30)

            if r.status_code != 200:
                log.warning("%s %s: HTTP %d", name, code, r.status_code)
                continue

            data = r.json()
            # Handle different response shapes
            items = (data.get("results") or data.get("instruments") or
                     data.get("hits", {}).get("hits") or data.get("data") or [])

            if isinstance(items, dict):
                items = items.get("hits", [])

            for item in items:
                if "_source" in item: item = item["_source"]
                fn      = str(item.get("instrumentNumber", item.get("docNumber", item.get("id", ""))))
                filed   = norm_date(item.get("recordedDate", item.get("fileDate", item.get("date", ""))))
                owner   = str(item.get("grantor", item.get("grantorName", item.get("owner", "")))).strip()
                grantee = str(item.get("grantee", item.get("granteeName", ""))).strip()
                amt     = parse_amount(item.get("amount", item.get("consideration", "")))
                legal   = str(item.get("legalDescription", item.get("legal", ""))).strip()
                url     = f"{base}/doc/{fn}" if fn else ""
                rec = blank_rec(name, fn, code, cat, cat_label, filed, owner, grantee, amt, legal, url)
                # Grab address if available
                for k in ["mailAddress","mailingAddress","grantorAddress","address"]:
                    if item.get(k):
                        rec["mail_address"] = str(item[k]).strip(); break
                records.append(rec)

            time.sleep(0.5)
        except Exception as e:
            log.warning("%s %s error: %s", name, code, e)

    log.info("%s: %d records", name, len(records))
    return records

def main():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== StackIQ Multi-County Scraper ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    all_records = []
    for name, host in COUNTIES.items():
        try:
            recs = scrape_county(name, host, cutoff, now)
            all_records.extend(recs)
        except Exception as e:
            log.error("%s failed: %s", name, e)

    seen, deduped = set(), []
    for r in all_records:
        key = f"{r['county']}|{r.get('doc_num')}|{r.get('filed')}|{r.get('owner')}"
        if key not in seen:
            seen.add(key); deduped.append(r)

    log.info("Total unique: %d", len(deduped))

    for r in deduped:
        try: r["score"], r["flags"] = compute_score(r, cutoff)
        except: r["score"] = 10; r["flags"] = []

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)

    payload = {
        "fetched_at": now.isoformat(),
        "source": "Multi-County TX Clerk Portals (publicsearch.us)",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "counties": list({r["county"] for r in deduped}),
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/multi_county_records.json","w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/multi_county_records.json","w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/multi_county_records.json")

    try:
        r = requests.post(API_URL, json=payload, timeout=120)
        log.info("API push: %d %s", r.status_code, r.text[:100])
    except Exception as e:
        log.warning("API push failed: %s", e)

    hot  = sum(1 for r in deduped if r.get("score",0) >= 70)
    warm = sum(1 for r in deduped if 40 <= r.get("score",0) < 70)
    log.info("=== Summary: Total=%d Hot=%d Warm=%d ===", len(deduped), hot, warm)
    for county in sorted({r["county"] for r in deduped}):
        count = sum(1 for r in deduped if r["county"] == county)
        log.info("  %s: %d", county, count)

if __name__ == "__main__":
    main()
