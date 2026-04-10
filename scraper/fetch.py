"""
Harris County Motivated Seller Lead Scraper v2
Targets specific Harris County Clerk Document Search Portal pages:
  - Real Property (RP.aspx)  — LP, liens, judgments, tax deeds, etc.
  - Foreclosures (FRCL_R.aspx)
  - Probate Court (CourtSearch.aspx?CaseType=Probate)
"""

import asyncio
import csv
import io
import json
import logging
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

BASE_URL  = "https://www.cclerk.hctx.net/Applications/WebSearch"
RP_URL    = f"{BASE_URL}/RP.aspx"
FRCL_URL  = f"{BASE_URL}/FRCL_R.aspx"
PROB_URL  = f"{BASE_URL}/CourtSearch.aspx?CaseType=Probate"

OUTPUT_DIRS   = [Path("dashboard"), Path("data")]
LOOKBACK_DAYS = 7
MAX_RETRIES   = 3
RETRY_DELAY   = 5

RP_INSTRUMENT_TYPES = [
    ("LP",       "LP",       "Lis Pendens"),
    ("RELLP",    "RELLP",    "Release Lis Pendens"),
    ("A/J",      "JUD",      "Abstract of Judgment"),
    ("CCJ",      "CCJ",      "Certified Judgment"),
    ("DRJUD",    "DRJUD",    "Domestic Relations Judgment"),
    ("LNIRS",    "LNIRS",    "IRS Lien"),
    ("LNFED",    "LNFED",    "Federal Lien"),
    ("LNCORPTX", "LNCORPTX", "Corp Tax Lien"),
    ("LN",       "LN",       "Lien"),
    ("LNMECH",   "LNMECH",   "Mechanic Lien"),
    ("LNHOA",    "LNHOA",    "HOA Lien"),
    ("MEDLN",    "MEDLN",    "Medicaid Lien"),
    ("TAXDEED",  "TAXDEED",  "Tax Deed"),
    ("NOC",      "NOC",      "Notice of Commencement"),
]


def parse_amount(text):
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def normalize_date(raw):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


def name_variants(name):
    name = name.strip().upper()
    variants = {name}
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            variants.add(f"{parts[1]} {parts[0]}")
    else:
        parts = name.split()
        if len(parts) >= 2:
            variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
            variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
    return variants


def compute_score(record, cutoff):
    flags = []
    score = 30
    cat    = record.get("cat", "")
    amount = record.get("amount")
    owner  = (record.get("owner") or "").upper()
    filed  = record.get("filed", "")

    if cat in ("LP", "RELLP"):        flags.append("Lis pendens")
    if cat == "NOFC":                 flags.append("Pre-foreclosure")
    if cat in ("JUD","CCJ","DRJUD"):  flags.append("Judgment lien")
    if cat in ("LNCORPTX","LNIRS","LNFED","TAXDEED"): flags.append("Tax lien")
    if cat == "LNMECH":               flags.append("Mechanic lien")
    if cat == "PRO":                  flags.append("Probate / estate")
    if re.search(r"\b(LLC|CORP|INC|LTD|TRUST)\b", owner): flags.append("LLC / corp owner")

    try:
        if datetime.strptime(filed[:10], "%Y-%m-%d") >= cutoff:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    score += len(flags) * 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    if amount:
        if amount > 100000: score += 15
        elif amount > 50000: score += 10
    if record.get("prop_address"):
        score += 5

    return min(score, 100), flags


class ParcelLookup:
    def __init__(self):
        self._by_name = {}

    def build(self, session):
        log.info("Building HCAD parcel lookup...")
        loaded = False
        for year in [datetime.now().year, datetime.now().year - 1]:
            if loaded:
                break
            for url in [
                f"https://pdata.hcad.org/GIS/Parcels/Parcels_{year}.zip",
                f"https://pdata.hcad.org/data/{year}/parcels.zip",
            ]:
                try:
                    r = session.get(url, timeout=120, stream=True)
                    if r.status_code == 200:
                        loaded = self._load_zip(r.content)
                        if loaded:
                            break
                except Exception:
                    pass
        if loaded:
            log.info("Parcel lookup ready: %d entries", len(self._by_name))
        else:
            log.warning("Parcel data unavailable")

    def lookup(self, owner):
        for v in name_variants(owner):
            hit = self._by_name.get(v)
            if hit:
                return hit
        return None

    def _load_zip(self, content):
        try:
            zf = zipfile.ZipFile(io.BytesIO(content))
        except Exception:
            return False
        for name in zf.namelist():
            lo = name.lower()
            if lo.endswith(".dbf") and HAS_DBF:
                return self._load_dbf(zf.read(name))
            if lo.endswith(".csv"):
                return self._load_csv(zf.read(name).decode("utf-8", errors="replace"))
        return False

    def _load_dbf(self, data):
        tmp = Path("/tmp/_parcels.dbf")
        tmp.write_bytes(data)
        try:
            for row in DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True):
                self._ingest(dict(row))
            return bool(self._by_name)
        except Exception as e:
            log.warning("DBF error: %s", e)
            return False

    def _load_csv(self, text):
        for row in csv.DictReader(io.StringIO(text)):
            self._ingest(row)
        return bool(self._by_name)

    def _ingest(self, row):
        def g(*keys):
            for k in keys:
                for variant in (k, k.upper(), k.lower()):
                    v = row.get(variant)
                    if v and str(v).strip():
                        return str(v).strip()
            return ""
        owner = g("OWNER","OWN1","OWNER1","OWNERNAME")
        if not owner:
            return
        info = {
            "prop_address": g("SITE_ADDR","SITEADDR"),
            "prop_city":    g("SITE_CITY","SITECITY"),
            "prop_state":   "TX",
            "prop_zip":     g("SITE_ZIP","SITEZIP"),
            "mail_address": g("ADDR_1","MAILADR1","MAIL_ADDR"),
            "mail_city":    g("CITY","MAILCITY"),
            "mail_state":   g("STATE","MAILSTATE") or "TX",
            "mail_zip":     g("ZIP","MAILZIP"),
        }
        for v in name_variants(owner):
            self._by_name.setdefault(v, info)


def parse_results_table(html, cat, cat_label):
    soup = BeautifulSoup(html, "lxml")
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        headers = [td.get_text(" ", strip=True).lower()
                   for td in rows[0].find_all(["th","td"])]
        joined = " ".join(headers)
        if not any(k in joined for k in ["instrument","grantor","file","date","type","case"]):
            continue

        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            try:
                data = {headers[i]: cells[i].get_text(" ", strip=True)
                        for i in range(min(len(headers), len(cells)))}

                link = None
                for cell in cells:
                    a = cell.find("a", href=True)
                    if a:
                        href = a["href"]
                        if not href.startswith("http"):
                            href = f"https://www.cclerk.hctx.net{href}"
                        link = href
                        break

                def find(*keys):
                    for k in keys:
                        for h in headers:
                            if k in h:
                                return data.get(h, "").strip()
                    return ""

                doc_num  = find("file number","instrument number","case number","file no")
                doc_type = find("instrument type","type","code")
                filed    = find("date filed","filed date","recording date","file date","date")
                grantor  = find("grantor","owner","debtor","party 1","applicant")
                grantee  = find("grantee","lender","creditor","party 2")
                legal    = find("legal","description","subdivision")
                amount   = find("amount","consideration","debt")

                if not doc_num and not grantor:
                    continue

                records.append({
                    "doc_num":      doc_num,
                    "doc_type":     doc_type or cat_label,
                    "cat":          cat,
                    "cat_label":    cat_label,
                    "filed":        normalize_date(filed) if filed else "",
                    "owner":        grantor,
                    "grantee":      grantee,
                    "amount":       parse_amount(amount),
                    "legal":        legal,
                    "clerk_url":    link or "",
                    "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                    "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                })
            except Exception as e:
                log.debug("Row parse error: %s", e)
    return records


async def fill_date_fields(page, start_date, end_date):
    for sel in ["#DateFrom","input[name*='DateFrom']","input[id*='DateFrom']",
                "input[name*='StartDate']","input[id*='Start']"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.fill(start_date)
                break
        except Exception:
            pass
    for sel in ["#DateTo","input[name*='DateTo']","input[id*='DateTo']",
                "input[name*='EndDate']","input[id*='End']"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.fill(end_date)
                break
        except Exception:
            pass


async def click_search(page):
    for sel in ["input[value='Search']","#SearchButton","button:has-text('Search')",
                "input[type='submit']","a:has-text('Search')"]:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.click()
                return
        except Exception:
            pass


async def paginate(page, cat, cat_label, max_pages=20):
    all_recs = []
    for pg in range(1, max_pages + 1):
        html = await page.content()
        recs = parse_results_table(html, cat, cat_label)
        all_recs.extend(recs)
        log.info("    page %d → %d records (total %d)", pg, len(recs), len(all_recs))
        if not recs:
            break
        advanced = False
        for sel in ["a:has-text('Next')","input[value='Next >']",
                    "a[id*='Next']","#lnkNext"]:
            try:
                nxt = page.locator(sel)
                if await nxt.count() > 0:
                    await nxt.click()
                    await page.wait_for_timeout(3000)
                    advanced = True
                    break
            except Exception:
                pass
        if not advanced:
            break
    return all_recs


async def scrape_rp(page, start_date, end_date, instrument_code, cat, cat_label):
    try:
        await page.goto(RP_URL, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(2000)
        await fill_date_fields(page, start_date, end_date)

        # Fill instrument type
        for sel in ["#InstrumentType","input[name*='InstrumentType']",
                    "input[id*='InstrumentType']","select[name*='Instrument']",
                    "select[id*='Instrument']"]:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    tag = await loc.evaluate("el => el.tagName")
                    if tag.upper() == "SELECT":
                        await loc.select_option(value=instrument_code)
                    else:
                        await loc.fill(instrument_code)
                    break
            except Exception:
                pass

        await click_search(page)
        await page.wait_for_timeout(4000)
        return await paginate(page, cat, cat_label)
    except Exception as e:
        log.warning("RP error [%s]: %s", instrument_code, e)
        return []


async def scrape_foreclosures(page, start_date, end_date):
    try:
        await page.goto(FRCL_URL, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(2000)
        await fill_date_fields(page, start_date, end_date)
        await click_search(page)
        await page.wait_for_timeout(4000)
        recs = await paginate(page, "NOFC", "Notice of Foreclosure")
        log.info("  Foreclosures → %d", len(recs))
        return recs
    except Exception as e:
        log.warning("Foreclosure error: %s", e)
        return []


async def scrape_probate(page, start_date, end_date):
    try:
        await page.goto(PROB_URL, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(2000)
        await fill_date_fields(page, start_date, end_date)
        await click_search(page)
        await page.wait_for_timeout(4000)
        recs = await paginate(page, "PRO", "Probate")
        log.info("  Probate → %d", len(recs))
        return recs
    except Exception as e:
        log.warning("Probate error: %s", e)
        return []


async def run_playwright(start_date, end_date):
    all_records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"],
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        page = await ctx.new_page()

        for code, cat, label in RP_INSTRUMENT_TYPES:
            log.info("Scraping RP.aspx → %s (%s)", code, label)
            recs = await scrape_rp(page, start_date, end_date, code, cat, label)
            all_records.extend(recs)
            await asyncio.sleep(1.5)

        log.info("Scraping foreclosures...")
        all_records.extend(await scrape_foreclosures(page, start_date, end_date))
        await asyncio.sleep(1.5)

        log.info("Scraping probate...")
        all_records.extend(await scrape_probate(page, start_date, end_date))

        await browser.close()
    return all_records


def export_ghl_csv(records, path):
    cols = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in records:
            parts = (r.get("owner") or "").strip().split()
            amt = r.get("amount")
            w.writerow({
                "First Name":             parts[0].title() if parts else "",
                "Last Name":              " ".join(parts[1:]).title() if len(parts) > 1 else "",
                "Mailing Address":        r.get("mail_address",""),
                "Mailing City":           r.get("mail_city",""),
                "Mailing State":          r.get("mail_state","TX"),
                "Mailing Zip":            r.get("mail_zip",""),
                "Property Address":       r.get("prop_address",""),
                "Property City":          r.get("prop_city",""),
                "Property State":         r.get("prop_state","TX"),
                "Property Zip":           r.get("prop_zip",""),
                "Lead Type":              r.get("cat_label",""),
                "Document Type":          r.get("doc_type",""),
                "Date Filed":             r.get("filed",""),
                "Document Number":        r.get("doc_num",""),
                "Amount/Debt Owed":       f"${amt:,.2f}" if amt else "",
                "Seller Score":           r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":                 "Harris County Clerk",
                "Public Records URL":     r.get("clerk_url",""),
            })
    log.info("GHL CSV saved → %s", path)


async def main():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start  = cutoff.strftime("%m/%d/%Y")
    end    = now.strftime("%m/%d/%Y")

    log.info("=== Harris County Lead Scraper v2 ===")
    log.info("Range: %s → %s", start, end)

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 LeadScraper/2.0"})

    parcel = ParcelLookup()
    parcel.build(session)

    if HAS_PLAYWRIGHT:
        raw = await run_playwright(start, end)
    else:
        log.error("Playwright not installed")
        raw = []

    # Deduplicate
    seen, deduped = set(), []
    for r in raw:
        key = r.get("doc_num") or f"{r.get('owner')}|{r.get('filed')}"
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)

    log.info("Unique records: %d", len(deduped))

    with_address = 0
    for r in deduped:
        try:
            addr = parcel.lookup(r.get("owner",""))
            if addr:
                r.update(addr)
                if r.get("prop_address"):
                    with_address += 1
            r["score"], r["flags"] = compute_score(r, cutoff)
        except Exception as e:
            log.debug("Enrich error: %s", e)
            r.setdefault("score", 30)
            r.setdefault("flags", [])

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)

    payload = {
        "fetched_at":   now.isoformat(),
        "source":       "Harris County Clerk – cclerk.hctx.net",
        "date_range":   {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total":        len(deduped),
        "with_address": with_address,
        "records":      deduped,
    }

    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        (d / "records.json").write_text(json.dumps(payload, indent=2, default=str))
        log.info("Saved → %s/records.json", d)

    export_ghl_csv(deduped, Path("data/leads_ghl.csv"))
    log.info("Done. total=%d with_address=%d", len(deduped), with_address)


if __name__ == "__main__":
    asyncio.run(main())
