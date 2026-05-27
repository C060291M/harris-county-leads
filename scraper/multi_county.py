"""
StackIQ — Multi-County TX Scraper (Production)
Orchestrates all county scrapers with date slicing + XHR interception.

Architecture:
- Base class handles XHR capture, date slicing, pagination, scoring
- Each county subclass only sets name/host/dept
- Date slicing bypasses 50-record cap completely
- Auto-adaptive: splits to half-day if daily slice hits limit
"""

import asyncio, json, logging, os, re, time
from datetime import datetime, timedelta
from typing import Optional
import requests
from playwright.async_api import async_playwright, Page

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("multi_county")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))
API_URL = os.getenv("API_URL", "https://api.stackiq.org/leads/bulk-import")

# ── Utilities ──────────────────────────────────────────────────────────────

def norm_date(raw) -> str:
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
        try: return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()

def parse_amount(raw) -> Optional[float]:
    if not raw: return None
    cleaned = re.sub(r"[^\d.]", "", str(raw))
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except: return None

def compute_score(r: dict, cutoff: datetime) -> tuple:
    s, flags = 0, []
    cat = r.get("cat", "")
    if cat in ("TAXDEED", "LNIRS", "LNFED"): flags.append("Tax Deed / IRS / Corp Lien"); s += 30
    if cat == "LNHOA": flags.append("HOA / Mechanic Lien"); s += 25
    if cat == "PRO": flags.append("Probate / Estate"); s += 20
    if cat in ("LN", "LNMECH", "LNSTATE"): flags.append("Lien on record"); s += 15
    if cat in ("LP", "NOFC", "NOFD"): flags.append("Lis Pendens / Pre-foreclosure"); s += 10
    if cat == "JUD": flags.append("Judgment Lien"); s += 10
    if cat in ("DIV", "BK"): flags.append("Divorce / Bankruptcy"); s += 10
    amt = r.get("amount")
    if amt and amt > 100000: flags.append("Amount > $100k"); s += 15
    elif amt and amt > 50000: flags.append("Amount > $50k"); s += 10
    filed = r.get("filed", "")
    if filed:
        try:
            if datetime.strptime(filed, "%Y-%m-%d") >= cutoff: flags.append("New this week"); s += 5
        except: pass
    if (r.get("mail_state") or "").upper().strip() not in ("", "TX"):
        flags.append("Absentee owner (out of state)"); s += 15
    if len(flags) >= 3: s += 10
    return min(s, 100), flags

def blank_rec(county, doc_num, doc_type, cat, cat_label, filed, owner,
              grantee="", amount=None, legal="", url="") -> dict:
    return {
        "doc_num": doc_num, "doc_type": doc_type, "cat": cat,
        "cat_label": cat_label, "filed": filed, "owner": owner,
        "grantee": grantee, "amount": amount, "legal": legal,
        "clerk_url": url, "county": county,
        "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
        "score": 0, "flags": [],
    }

def date_slices(start: datetime, end: datetime, days: int = 1):
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt

# ── Doc Types ──────────────────────────────────────────────────────────────

DOC_TYPES = [
    ("Lis Pendens",           "LP",      "Lis Pendens"),
    ("Tax Deed",              "TAXDEED", "Tax Deed"),
    ("Abstract of Judgment",  "JUD",     "Abstract of Judgment"),
    ("Mechanic Lien",         "LNMECH",  "Mechanic Lien"),
    ("Federal Tax Lien",      "LNFED",   "Federal Tax Lien"),
    ("State Tax Lien",        "LNSTATE", "State Tax Lien"),
    ("HOA Lien",              "LNHOA",   "HOA Lien"),
    ("Notice of Foreclosure", "NOFC",    "Notice of Foreclosure"),
    ("IRS Lien",              "LNIRS",   "IRS Lien"),
    ("Probate",               "PRO",     "Probate"),
    ("Divorce Decree",        "DIV",     "Divorce"),
]

# ── Base Scraper ───────────────────────────────────────────────────────────

class PublicSearchScraper:
    """
    Scraper for counties on publicsearch.us (Neumo platform).
    Step 1: Launch browser once, search, intercept XHR API call
    Step 2: Extract endpoint + headers + payload format
    Step 3: Replay directly via requests with daily date slicing
    Step 4: If daily slice = 50 records, auto-split to 12-hour windows
    """
    county: str = ""
    host: str = ""
    dept_name: str = ""

    def __init__(self):
        self.base_url = f"https://{self.host}"
        self._api_url = None
        self._api_headers = {}
        self._api_method = "POST"
        self._api_post_template = ""
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, */*",
            "Referer": f"{self.base_url}/",
            "Origin": self.base_url,
        })

    async def scrape(self, start: datetime, end: datetime) -> list:
        log.info("%s - scraping %s to %s",
                 self.county, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

        # Capture API endpoint via browser
        await self._capture_api_endpoint(start)

        all_records = []
        for doc_type, cat, cat_label in DOC_TYPES:
            try:
                recs = await self._scrape_doc_type(doc_type, cat, cat_label, start, end)
                all_records.extend(recs)
                if recs:
                    log.info("%s %s: %d records", self.county, doc_type, len(recs))
            except Exception as e:
                log.warning("%s %s error: %s", self.county, doc_type, e)

        log.info("%s total: %d records", self.county, len(all_records))
        return all_records

    async def _capture_api_endpoint(self, start: datetime):
        """Run browser once to capture the XHR API call signature."""
        start_str = start.strftime("%m/%d/%Y")
        end_str = (start + timedelta(days=1)).strftime("%m/%d/%Y")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 900}
            )
            page = await context.new_page()
            api_calls = []

            async def on_request(req):
                url = req.url
                if any(x in url for x in ["/api/", "/search", "/instruments"]):
                    if "google" not in url and "analytics" not in url:
                        try:
                            api_calls.append({
                                "url": url,
                                "method": req.method,
                                "headers": dict(req.headers),
                                "post_data": req.post_data or "",
                            })
                        except: pass

            page.on("request", on_request)

            try:
                await page.goto(f"{self.base_url}/search/advanced",
                               wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(2000)
                await self._select_dept(page)
                await page.fill("#recordedDateRange-start", start_str)
                await page.wait_for_timeout(200)
                await page.fill("#recordedDateRange-end", end_str)
                await page.wait_for_timeout(200)
                await self._select_doc_type(page, "Lis Pendens")
                await page.click("button:has-text('Search')", timeout=5000)
                await page.wait_for_timeout(5000)
            except Exception as e:
                log.warning("%s capture error: %s", self.county, e)
            finally:
                await browser.close()

            for call in reversed(api_calls):
                if call.get("method") in ("POST", "GET"):
                    self._api_url = call["url"]
                    self._api_headers = {
                        k: v for k, v in call["headers"].items()
                        if k.lower() not in ("content-length",)
                    }
                    self._api_method = call["method"]
                    self._api_post_template = call.get("post_data", "")
                    log.info("%s API captured: %s %s",
                             self.county, self._api_method, self._api_url)
                    break

            if not self._api_url:
                log.warning("%s: no API endpoint captured", self.county)

    async def _select_dept(self, page: Page):
        try:
            await page.click("#department", timeout=5000)
            await page.wait_for_timeout(1000)
            await page.evaluate(f"""
                () => {{
                    const opts = Array.from(document.querySelectorAll(
                        '[class*="option"], li[role="option"], [class*="item"]'
                    )).filter(o => {{
                        const t = o.textContent.trim();
                        return t.length > 0 && t.length < 40;
                    }});
                    const target = opts.find(o => o.textContent.trim() === "{self.dept_name}");
                    if (target) {{ target.click(); return; }}
                    if (opts.length > 0) opts[0].click();
                }}
            """)
            await page.wait_for_timeout(500)
        except: pass

    async def _select_doc_type(self, page: Page, doc_type: str):
        try:
            await page.click("#docTypes-input")
            await page.wait_for_timeout(200)
            await page.type("#docTypes-input", doc_type, delay=30)
            await page.wait_for_timeout(1500)
            option = await page.wait_for_selector(
                ".react-tokenized-select__option, [role='option']",
                timeout=3000
            )
            if option:
                await option.click()
                await page.wait_for_timeout(300)
        except: pass

    async def _scrape_doc_type(self, doc_type: str, cat: str, cat_label: str,
                               start: datetime, end: datetime) -> list:
        records = []
        for slice_start, slice_end in date_slices(start, end, days=1):
            s = slice_start.strftime("%m/%d/%Y")
            e = slice_end.strftime("%m/%d/%Y")
            slice_recs = self._fetch_direct(doc_type, cat, cat_label, s, e)

            # Auto-split if at limit
            if len(slice_recs) >= 50:
                log.info("%s %s %s: hit limit, splitting to 12hr", self.county, doc_type, s)
                slice_recs = []
                mid = slice_start + timedelta(hours=12)
                for sub_s, sub_e in [(slice_start, mid), (mid, slice_end)]:
                    sub_recs = self._fetch_direct(
                        doc_type, cat, cat_label,
                        sub_s.strftime("%m/%d/%Y"),
                        sub_e.strftime("%m/%d/%Y")
                    )
                    slice_recs.extend(sub_recs)

            records.extend(slice_recs)
        return records

    def _fetch_direct(self, doc_type: str, cat: str, cat_label: str,
                      start_str: str, end_str: str) -> list:
        if not self._api_url:
            return []

        records = []
        offset = 0
        limit = 200

        while True:
            try:
                payload = self._build_payload(doc_type, start_str, end_str, offset, limit)

                if self._api_method == "POST":
                    r = self._session.post(
                        self._api_url, json=payload,
                        headers=self._api_headers, timeout=30
                    )
                else:
                    r = self._session.get(
                        self._api_url, params=payload,
                        headers=self._api_headers, timeout=30
                    )

                if r.status_code != 200:
                    break

                items = self._extract_items(r.json())
                if not items:
                    break

                for item in items:
                    rec = self._parse_item(item, cat, cat_label, doc_type)
                    if rec:
                        records.append(rec)

                if len(items) < limit:
                    break
                offset += limit
                time.sleep(0.3)

            except Exception as e:
                log.warning("%s direct fetch error: %s", self.county, e)
                break

        return records

    def _build_payload(self, doc_type: str, start_str: str, end_str: str,
                       offset: int, limit: int) -> dict:
        if self._api_post_template:
            try:
                base = json.loads(self._api_post_template)
                for k in list(base.keys()):
                    kl = k.lower()
                    if "doctype" in kl or ("type" in kl and "date" not in kl):
                        base[k] = [doc_type] if isinstance(base[k], list) else doc_type
                    elif "start" in kl:
                        base[k] = start_str
                    elif "end" in kl:
                        base[k] = end_str
                    elif "offset" in kl:
                        base[k] = offset
                    elif "limit" in kl or "size" in kl:
                        base[k] = limit
                return base
            except: pass

        return {
            "docTypes": [doc_type],
            "dateField": "RecordedDate",
            "startDate": start_str,
            "endDate": end_str,
            "offset": offset,
            "limit": limit,
            "sort": "desc",
        }

    def _extract_items(self, data) -> list:
        if isinstance(data, list): return data
        if isinstance(data, dict):
            for key in ("results", "instruments", "hits", "data", "records", "items"):
                val = data.get(key)
                if isinstance(val, list): return val
                if isinstance(val, dict):
                    hits = val.get("hits", [])
                    if isinstance(hits, list): return hits
        return []

    def _parse_item(self, item: dict, cat: str, cat_label: str, doc_type: str) -> Optional[dict]:
        if "_source" in item: item = item["_source"]
        fn      = str(item.get("instrumentNumber", item.get("docNumber", item.get("id", "")))).strip()
        filed   = norm_date(item.get("recordedDate", item.get("fileDate", item.get("date", ""))))
        owner   = str(item.get("grantor", item.get("grantorName", item.get("owner", "")))).strip()
        grantee = str(item.get("grantee", item.get("granteeName", ""))).strip()
        amt     = parse_amount(item.get("amount", item.get("consideration", "")))
        legal   = str(item.get("legalDescription", item.get("legal", ""))).strip()
        url     = f"{self.base_url}/doc/{fn}" if fn else ""
        rec = blank_rec(self.county, fn, doc_type, cat, cat_label, filed, owner, grantee, amt, legal, url)
        for k in ["mailAddress", "mailingAddress", "grantorAddress", "address"]:
            if item.get(k):
                rec["mail_address"] = str(item[k]).strip()
                break
        return rec


# ── County Adapters ────────────────────────────────────────────────────────

class DallasScraper(PublicSearchScraper):
    county    = "Dallas"
    host      = "dallas.tx.publicsearch.us"
    dept_name = "Property Records"

class TarrantScraper(PublicSearchScraper):
    county    = "Tarrant"
    host      = "tarrant.tx.publicsearch.us"
    dept_name = "Real Property"

class BexarScraper(PublicSearchScraper):
    county    = "Bexar"
    host      = "bexar.tx.publicsearch.us"
    dept_name = "Land Records"

class CollinScraper(PublicSearchScraper):
    county    = "Collin"
    host      = "collin.tx.publicsearch.us"
    dept_name = "Property Records"


# ── Main ──────────────────────────────────────────────────────────────────

async def main():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== StackIQ Multi-County Scraper (Production) ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    scrapers = [DallasScraper(), TarrantScraper(), BexarScraper(), CollinScraper()]
    all_records = []

    for scraper in scrapers:
        try:
            recs = await scraper.scrape(cutoff, now)
            all_records.extend(recs)
        except Exception as e:
            log.error("%s failed: %s", scraper.county, e)

    seen, deduped = set(), []
    for r in all_records:
        key = f"{r['county']}|{r.get('doc_num') or r.get('owner','') + r.get('filed','')}"
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    log.info("Total unique: %d", len(deduped))

    for r in deduped:
        try: r["score"], r["flags"] = compute_score(r, cutoff)
        except: r["score"] = 10; r["flags"] = []

    deduped.sort(key=lambda x: x.get("score", 0), reverse=True)

    payload = {
        "fetched_at": now.isoformat(),
        "source": "Multi-County TX Clerk Portals",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "counties": list({r["county"] for r in deduped}),
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/multi_county_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/multi_county_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/multi_county_records.json")

    try:
        r = requests.post(API_URL, json=payload, timeout=120)
        log.info("API push: %d %s", r.status_code, r.text[:100])
    except Exception as e:
        log.warning("API push failed: %s", e)

    hot  = sum(1 for r in deduped if r.get("score", 0) >= 70)
    warm = sum(1 for r in deduped if 40 <= r.get("score", 0) < 70)
    log.info("=== Summary: Total=%d Hot=%d Warm=%d ===", len(deduped), hot, warm)
    for county in sorted({r["county"] for r in deduped}):
        count = sum(1 for r in deduped if r["county"] == county)
        log.info("  %s: %d", county, count)

if __name__ == "__main__":
    asyncio.run(main())
