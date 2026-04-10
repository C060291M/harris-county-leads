"""
Harris County Motivated Seller Lead Scraper
Fetches public records from Harris County Clerk portal (last 7 days)
and enriches with property appraiser parcel data.
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Playwright import (graceful fallback for environments without it)
# ---------------------------------------------------------------------------
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

# ---------------------------------------------------------------------------
# Optional dbfread
# ---------------------------------------------------------------------------
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CLERK_URL = "https://www.cclerk.hctx.net/PublicRecords.aspx"
HCAD_BULK_BASE = "https://pdata.hcad.org"
HCAD_BULK_PAGE = "https://pdata.hcad.org/download/2024.aspx"  # fallback
HCAD_DIRECT_ZIP = (
    "https://pdata.hcad.org/GIS/Parcels/Parcels_2024.zip"
)  # attempt
OUTPUT_DIRS = [
    Path("dashboard"),
    Path("data"),
]
LOOKBACK_DAYS = 7
MAX_RETRIES = 3
RETRY_DELAY = 4  # seconds

# Document type → category mapping
DOC_TYPE_MAP = {
    # Lis Pendens
    "LP": ("LP", "Lis Pendens"),
    "LIS PENDENS": ("LP", "Lis Pendens"),
    "LISPEND": ("LP", "Lis Pendens"),
    # Foreclosure
    "NOFC": ("NOFC", "Notice of Foreclosure"),
    "NOF": ("NOFC", "Notice of Foreclosure"),
    "NOTICE OF FORECLOSURE": ("NOFC", "Notice of Foreclosure"),
    "FORECLOSURE": ("NOFC", "Notice of Foreclosure"),
    # Tax Deed
    "TAXDEED": ("TAXDEED", "Tax Deed"),
    "TAX DEED": ("TAXDEED", "Tax Deed"),
    # Judgments
    "JUD": ("JUD", "Judgment"),
    "JUDGMENT": ("JUD", "Judgment"),
    "CCJ": ("CCJ", "Certified Judgment"),
    "CERTIFIED JUDGMENT": ("CCJ", "Certified Judgment"),
    "DRJUD": ("DRJUD", "Domestic Judgment"),
    "DOMESTIC JUDGMENT": ("DRJUD", "Domestic Judgment"),
    # Liens – corporate / tax
    "LNCORPTX": ("LNCORPTX", "Corp Tax Lien"),
    "CORP TAX LIEN": ("LNCORPTX", "Corp Tax Lien"),
    "LNIRS": ("LNIRS", "IRS Lien"),
    "IRS LIEN": ("LNIRS", "IRS Lien"),
    "LNFED": ("LNFED", "Federal Lien"),
    "FEDERAL LIEN": ("LNFED", "Federal Lien"),
    # Liens – general
    "LN": ("LN", "Lien"),
    "LIEN": ("LN", "Lien"),
    "LNMECH": ("LNMECH", "Mechanic Lien"),
    "MECHANIC LIEN": ("LNMECH", "Mechanic Lien"),
    "LNHOA": ("LNHOA", "HOA Lien"),
    "HOA LIEN": ("LNHOA", "HOA Lien"),
    # Medical / Medicaid
    "MEDLN": ("MEDLN", "Medicaid Lien"),
    "MEDICAID LIEN": ("MEDLN", "Medicaid Lien"),
    # Probate
    "PRO": ("PRO", "Probate"),
    "PROBATE": ("PRO", "Probate"),
    # Notice of Commencement
    "NOC": ("NOC", "Notice of Commencement"),
    "NOTICE OF COMMENCEMENT": ("NOC", "Notice of Commencement"),
    # Release Lis Pendens
    "RELLP": ("RELLP", "Release Lis Pendens"),
    "RELEASE LIS PENDENS": ("RELLP", "Release Lis Pendens"),
}

TARGET_CATS = {
    "LP", "NOFC", "TAXDEED", "JUD", "CCJ", "DRJUD",
    "LNCORPTX", "LNIRS", "LNFED", "LN", "LNMECH", "LNHOA",
    "MEDLN", "PRO", "NOC", "RELLP",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def retry(fn, *args, attempts=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """Synchronous retry wrapper."""
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, attempts, exc)
            if attempt < attempts:
                time.sleep(delay)
    log.error("All %d attempts failed for %s", attempts, fn.__name__)
    return None


def safe_get(url: str, session: requests.Session, **kwargs) -> Optional[requests.Response]:
    """GET with retry."""
    def _get():
        r = session.get(url, timeout=30, **kwargs)
        r.raise_for_status()
        return r
    return retry(_get)


def parse_amount(text: str) -> Optional[float]:
    """Extract dollar amount from a string."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def name_variants(full_name: str):
    """Return lookup variants for owner name matching."""
    full_name = full_name.strip().upper()
    variants = {full_name}
    # Try splitting on comma: "LAST, FIRST"
    if "," in full_name:
        parts = [p.strip() for p in full_name.split(",", 1)]
        variants.add(f"{parts[1]} {parts[0]}")  # FIRST LAST
        variants.add(full_name)
    else:
        parts = full_name.split()
        if len(parts) >= 2:
            variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")  # LAST, FIRST
            variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")   # LAST FIRST
    return variants


def compute_score(record: dict, cutoff_date: datetime) -> tuple[int, list[str]]:
    """Compute seller score 0–100 and return (score, flags)."""
    flags = []
    score = 30  # base

    cat = record.get("cat", "")
    amount = record.get("amount")
    filed_str = record.get("filed", "")
    owner = (record.get("owner") or "").upper()
    has_address = bool(record.get("prop_address"))

    # Flags
    if cat in ("LP", "RELLP"):
        flags.append("Lis pendens")
    if cat == "NOFC":
        flags.append("Pre-foreclosure")
    if cat in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
    if cat in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if cat == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "PRO":
        flags.append("Probate / estate")
    if re.search(r"\b(LLC|CORP|INC|LTD|LP|TRUST)\b", owner):
        flags.append("LLC / corp owner")

    # Date "new this week"
    try:
        filed_dt = datetime.strptime(filed_str[:10], "%Y-%m-%d")
        if filed_dt >= cutoff_date:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    score += len(flags) * 10  # +10 per flag (before combo / amount bonuses)

    # LP + FC combo
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

    # Amount bonuses
    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10

    # Address bonus
    if has_address:
        score += 5

    return min(score, 100), flags

# ---------------------------------------------------------------------------
# Parcel / HCAD loader
# ---------------------------------------------------------------------------

class ParcelLookup:
    """
    Attempts to download HCAD bulk parcel data (DBF or CSV inside ZIP)
    and builds an owner-name → address lookup.
    """

    def __init__(self):
        self._by_name: dict[str, dict] = {}

    # ---- public API --------------------------------------------------------

    def build(self, session: requests.Session):
        log.info("Building HCAD parcel lookup …")
        loaded = False
        for attempt_fn in [self._try_direct_zip, self._try_bulk_page]:
            try:
                loaded = attempt_fn(session)
                if loaded:
                    break
            except Exception as exc:
                log.warning("Parcel attempt failed: %s", exc)

        if not loaded:
            log.warning("Could not load parcel data — address enrichment disabled.")
        else:
            log.info("Parcel lookup ready: %d owner entries.", len(self._by_name))

    def lookup(self, owner_name: str) -> Optional[dict]:
        """Return address info dict or None."""
        for variant in name_variants(owner_name):
            hit = self._by_name.get(variant)
            if hit:
                return hit
        return None

    # ---- private -----------------------------------------------------------

    def _try_direct_zip(self, session: requests.Session) -> bool:
        """Try to download HCAD parcel zip directly."""
        # HCAD publishes parcels at pdata.hcad.org; the year changes.
        # We try current and previous year.
        for year in [datetime.now().year, datetime.now().year - 1]:
            urls_to_try = [
                f"https://pdata.hcad.org/GIS/Parcels/Parcels_{year}.zip",
                f"https://pdata.hcad.org/data/{year}/parcels.zip",
            ]
            for url in urls_to_try:
                try:
                    log.info("Trying parcel zip: %s", url)
                    resp = session.get(url, timeout=120, stream=True)
                    if resp.status_code == 200:
                        return self._load_zip_bytes(resp.content)
                except Exception:
                    pass
        return False

    def _try_bulk_page(self, session: requests.Session) -> bool:
        """Scrape HCAD bulk download page to find zip link."""
        for year in [datetime.now().year, datetime.now().year - 1]:
            page_url = f"https://pdata.hcad.org/download/{year}.aspx"
            try:
                resp = session.get(page_url, timeout=30)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "parcel" in href.lower() and href.endswith(".zip"):
                        full = href if href.startswith("http") else f"https://pdata.hcad.org{href}"
                        log.info("Found parcel zip link: %s", full)
                        r2 = session.get(full, timeout=120, stream=True)
                        if r2.status_code == 200:
                            return self._load_zip_bytes(r2.content)
            except Exception as exc:
                log.warning("Bulk page attempt failed: %s", exc)
        return False

    def _load_zip_bytes(self, content: bytes) -> bool:
        try:
            zf = zipfile.ZipFile(io.BytesIO(content))
        except Exception:
            return False

        # Look for DBF or CSV inside
        for name in zf.namelist():
            lower = name.lower()
            if lower.endswith(".dbf") and HAS_DBF:
                return self._load_dbf(zf.read(name))
            if lower.endswith(".csv"):
                return self._load_csv(zf.read(name).decode("utf-8", errors="replace"))
        return False

    def _load_dbf(self, data: bytes) -> bool:
        tmp = Path("/tmp/parcels.dbf")
        tmp.write_bytes(data)
        try:
            db = DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True)
            for row in db:
                self._ingest_row(dict(row))
            return bool(self._by_name)
        except Exception as exc:
            log.warning("DBF load error: %s", exc)
            return False

    def _load_csv(self, text: str) -> bool:
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            self._ingest_row(row)
        return bool(self._by_name)

    def _ingest_row(self, row: dict):
        def get(*keys):
            for k in keys:
                v = row.get(k) or row.get(k.upper()) or row.get(k.lower())
                if v and str(v).strip():
                    return str(v).strip()
            return ""

        owner = get("OWNER", "OWN1", "OWNER1")
        site_addr = get("SITE_ADDR", "SITEADDR", "SITE_ADDRESS")
        site_city = get("SITE_CITY", "SITECITY")
        site_zip = get("SITE_ZIP", "SITEZIP")
        mail_addr = get("ADDR_1", "MAILADR1", "MAIL_ADDR", "MAIL_ADDRESS")
        mail_city = get("CITY", "MAILCITY", "MAIL_CITY")
        mail_state = get("STATE", "MAILSTATE", "MAIL_STATE")
        mail_zip = get("ZIP", "MAILZIP", "MAIL_ZIP")

        if not owner:
            return

        info = {
            "prop_address": site_addr,
            "prop_city": site_city,
            "prop_state": "TX",
            "prop_zip": site_zip,
            "mail_address": mail_addr,
            "mail_city": mail_city,
            "mail_state": mail_state or "TX",
            "mail_zip": mail_zip,
        }

        for variant in name_variants(owner):
            self._by_name.setdefault(variant, info)

# ---------------------------------------------------------------------------
# Clerk scraper (Playwright)
# ---------------------------------------------------------------------------

async def scrape_clerk_playwright(start_date: str, end_date: str) -> list[dict]:
    """
    Use Playwright to interact with the Harris County Clerk public records portal.
    Returns list of raw record dicts.
    """
    if not HAS_PLAYWRIGHT:
        log.error("Playwright not installed — cannot scrape clerk portal.")
        return []

    records = []
    log.info("Launching Playwright for clerk portal …")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        try:
            log.info("Navigating to clerk portal …")
            await page.goto(CLERK_URL, wait_until="networkidle", timeout=60_000)
            await page.wait_for_timeout(2000)

            # --- Fill date range ---
            # Try to find date inputs; field names vary by portal version
            for field_id, value in [
                ("txtStartDate", start_date),
                ("ctl00_ContentPlaceHolder1_txtStartDate", start_date),
                ("txtEndDate", end_date),
                ("ctl00_ContentPlaceHolder1_txtEndDate", end_date),
            ]:
                try:
                    loc = page.locator(f"#{field_id}")
                    if await loc.count() > 0:
                        await loc.fill(value)
                except Exception:
                    pass

            # Try to set document type to "ALL" or submit search
            # The portal uses __doPostBack / ASP.NET patterns
            for btn_id in [
                "btnSearch",
                "ctl00_ContentPlaceHolder1_btnSearch",
                "btnSubmit",
                "ctl00_ContentPlaceHolder1_btnSubmit",
            ]:
                try:
                    btn = page.locator(f"#{btn_id}")
                    if await btn.count() > 0:
                        await btn.click()
                        break
                except Exception:
                    pass

            await page.wait_for_timeout(5000)

            # --- Iterate through pages ---
            page_num = 0
            while True:
                page_num += 1
                log.info("Parsing clerk results page %d …", page_num)
                html = await page.content()
                new_records = _parse_clerk_html(html, start_date)
                records.extend(new_records)
                log.info("  Found %d records on page %d", len(new_records), page_num)

                # Try to click "Next" page
                advanced = False
                for next_sel in ["a:has-text('Next')", "input[value='Next']", "#lnkNext", "#btnNext"]:
                    try:
                        nxt = page.locator(next_sel)
                        if await nxt.count() > 0:
                            await nxt.click()
                            await page.wait_for_timeout(3000)
                            advanced = True
                            break
                    except Exception:
                        pass

                if not advanced or page_num > 50:
                    break

        except PWTimeout as exc:
            log.error("Playwright timeout: %s", exc)
        except Exception as exc:
            log.error("Playwright error: %s", exc)
        finally:
            await browser.close()

    log.info("Clerk scrape complete — %d raw records.", len(records))
    return records


def _parse_clerk_html(html: str, start_date: str) -> list[dict]:
    """Parse HTML from clerk portal results table."""
    soup = BeautifulSoup(html, "lxml")
    records = []

    # The portal renders results in a GridView / table
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        if not any(h in headers for h in ["document", "doc", "type", "filed", "grantor"]):
            continue

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            try:
                row_data = {h: (cells[i].get_text(strip=True) if i < len(cells) else "")
                            for i, h in enumerate(headers)}
                # Extract hyperlink if present
                link = None
                for cell in cells:
                    a = cell.find("a", href=True)
                    if a:
                        href = a["href"]
                        if not href.startswith("http"):
                            href = "https://www.cclerk.hctx.net/" + href.lstrip("/")
                        link = href
                        break

                rec = _normalize_clerk_row(row_data, link)
                if rec:
                    records.append(rec)
            except Exception as exc:
                log.debug("Row parse error: %s", exc)

    return records


def _normalize_clerk_row(row: dict, link: Optional[str]) -> Optional[dict]:
    """Map raw clerk HTML row → normalized record dict."""
    def find(keys):
        for k in keys:
            v = row.get(k) or row.get(k.replace(" ", "")) or row.get(k.upper())
            if v:
                return v.strip()
        return ""

    doc_num = find(["document number", "doc number", "docnumber", "doc#", "instrument", "instrument number"])
    doc_type_raw = find(["document type", "doc type", "doctype", "type"])
    filed = find(["filed date", "date filed", "fileddate", "recording date", "date recorded"])
    grantor = find(["grantor", "owner", "seller"])
    grantee = find(["grantee", "buyer", "lender"])
    legal = find(["legal description", "legal desc", "legal"])
    amount = find(["amount", "consideration", "debt"])

    if not doc_type_raw:
        return None

    cat, cat_label = _classify_doc_type(doc_type_raw)
    if cat not in TARGET_CATS:
        return None

    return {
        "doc_num": doc_num,
        "doc_type": doc_type_raw,
        "cat": cat,
        "cat_label": cat_label,
        "filed": _normalize_date(filed),
        "owner": grantor,
        "grantee": grantee,
        "amount": parse_amount(amount),
        "legal": legal,
        "clerk_url": link or "",
        # To be enriched later
        "prop_address": "",
        "prop_city": "",
        "prop_state": "TX",
        "prop_zip": "",
        "mail_address": "",
        "mail_city": "",
        "mail_state": "TX",
        "mail_zip": "",
    }


def _classify_doc_type(raw: str) -> tuple[str, str]:
    key = raw.strip().upper()
    if key in DOC_TYPE_MAP:
        return DOC_TYPE_MAP[key]
    # Partial match
    for token, (cat, label) in DOC_TYPE_MAP.items():
        if token in key:
            return cat, label
    return ("OTHER", raw)


def _normalize_date(raw: str) -> str:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()

# ---------------------------------------------------------------------------
# Fallback: static HTTP scrape of clerk portal (no Playwright)
# ---------------------------------------------------------------------------

def scrape_clerk_static(start_date: str, end_date: str, session: requests.Session) -> list[dict]:
    """
    Attempt a direct HTTP POST to the clerk portal (ASP.NET __doPostBack).
    This is a best-effort fallback when Playwright is unavailable.
    """
    log.info("Attempting static HTTP scrape of clerk portal …")
    records = []

    try:
        resp = session.get(CLERK_URL, timeout=30)
        soup = BeautifulSoup(resp.text, "lxml")

        viewstate = soup.find("input", {"name": "__VIEWSTATE"})
        eventvalidation = soup.find("input", {"name": "__EVENTVALIDATION"})
        viewstategenerator = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})

        form_data = {
            "__VIEWSTATE": viewstate["value"] if viewstate else "",
            "__EVENTVALIDATION": eventvalidation["value"] if eventvalidation else "",
            "__VIEWSTATEGENERATOR": viewstategenerator["value"] if viewstategenerator else "",
            "txtStartDate": start_date,
            "txtEndDate": end_date,
            "btnSearch": "Search",
        }

        # Add any hidden inputs
        for inp in soup.find_all("input", {"type": "hidden"}):
            name = inp.get("name", "")
            if name and name not in form_data:
                form_data[name] = inp.get("value", "")

        post_resp = session.post(CLERK_URL, data=form_data, timeout=30)
        recs = _parse_clerk_html(post_resp.text, start_date)
        records.extend(recs)
        log.info("Static scrape returned %d records.", len(records))

    except Exception as exc:
        log.error("Static scrape error: %s", exc)

    return records

# ---------------------------------------------------------------------------
# GHL CSV export
# ---------------------------------------------------------------------------

def export_ghl_csv(records: list[dict], path: Path):
    fieldnames = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            owner = r.get("owner", "") or ""
            # Split owner into first / last (best effort)
            parts = owner.strip().split()
            first = parts[0].title() if parts else ""
            last = " ".join(parts[1:]).title() if len(parts) > 1 else ""

            amount = r.get("amount")
            writer.writerow({
                "First Name": first,
                "Last Name": last,
                "Mailing Address": r.get("mail_address", ""),
                "Mailing City": r.get("mail_city", ""),
                "Mailing State": r.get("mail_state", "TX"),
                "Mailing Zip": r.get("mail_zip", ""),
                "Property Address": r.get("prop_address", ""),
                "Property City": r.get("prop_city", ""),
                "Property State": r.get("prop_state", "TX"),
                "Property Zip": r.get("prop_zip", ""),
                "Lead Type": r.get("cat_label", ""),
                "Document Type": r.get("doc_type", ""),
                "Date Filed": r.get("filed", ""),
                "Document Number": r.get("doc_num", ""),
                "Amount/Debt Owed": f"${amount:,.2f}" if amount else "",
                "Seller Score": r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source": "Harris County Clerk",
                "Public Records URL": r.get("clerk_url", ""),
            })
    log.info("GHL CSV saved → %s", path)

# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start_date = cutoff.strftime("%m/%d/%Y")
    end_date = now.strftime("%m/%d/%Y")
    start_date_iso = cutoff.strftime("%Y-%m-%d")

    log.info("=== Harris County Lead Scraper ===")
    log.info("Date range: %s → %s", start_date, end_date)

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    })

    # 1. Build parcel lookup
    parcel = ParcelLookup()
    parcel.build(session)

    # 2. Scrape clerk portal
    if HAS_PLAYWRIGHT:
        raw_records = await scrape_clerk_playwright(start_date, end_date)
    else:
        raw_records = scrape_clerk_static(start_date, end_date, session)

    # Deduplicate by doc_num
    seen = set()
    deduped = []
    for r in raw_records:
        key = r.get("doc_num") or id(r)
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    log.info("Unique records after dedup: %d", len(deduped))

    # 3. Enrich with parcel data + scoring
    with_address = 0
    for r in deduped:
        try:
            owner = r.get("owner", "")
            if owner:
                addr = parcel.lookup(owner)
                if addr:
                    r.update(addr)
                    if r.get("prop_address"):
                        with_address += 1

            score, flags = compute_score(r, cutoff)
            r["score"] = score
            r["flags"] = flags
        except Exception as exc:
            log.debug("Enrichment error for %s: %s", r.get("doc_num"), exc)
            r.setdefault("score", 30)
            r.setdefault("flags", [])

    # Sort by score descending
    deduped.sort(key=lambda x: x.get("score", 0), reverse=True)

    # 4. Build output payload
    payload = {
        "fetched_at": now.isoformat(),
        "source": "Harris County Clerk – cclerk.hctx.net",
        "date_range": {"start": start_date_iso, "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "with_address": with_address,
        "records": deduped,
    }

    # 5. Save JSON outputs
    for out_dir in OUTPUT_DIRS:
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / "records.json"
        json_path.write_text(json.dumps(payload, indent=2, default=str))
        log.info("JSON saved → %s", json_path)

    # 6. GHL CSV export
    csv_path = Path("data") / "leads_ghl.csv"
    export_ghl_csv(deduped, csv_path)

    log.info("Done. Total=%d  WithAddress=%d", len(deduped), with_address)
    return payload


if __name__ == "__main__":
    asyncio.run(main())
