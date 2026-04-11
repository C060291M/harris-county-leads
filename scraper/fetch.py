"""
Harris County Motivated Seller Lead Scraper v4
Uses direct HTTP POST (requests + BeautifulSoup) for ASP.NET WebForms.
Saves debug HTML as artifacts for troubleshooting.
"""
import csv, io, json, logging, re, sys, time, zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

BASE  = "https://www.cclerk.hctx.net/Applications/WebSearch"
RP    = f"{BASE}/RP.aspx"
FRCL  = f"{BASE}/FRCL_R.aspx"
PROB  = f"{BASE}/CourtSearch.aspx?CaseType=Probate"

OUTPUT_DIRS   = [Path("dashboard"), Path("data")]
DEBUG_DIR     = Path("debug")
LOOKBACK_DAYS = 7

INSTRUMENT_TYPES = [
    ("LP",       "LP",       "Lis Pendens"),
    ("RELLP",    "RELLP",    "Release Lis Pendens"),
    ("A/J",      "JUD",      "Abstract of Judgment"),
    ("CCJ",      "CCJ",      "Certified Judgment"),
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

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
})


# ── helpers ──────────────────────────────────────────────────────────────────

def save_debug(name: str, html: str):
    DEBUG_DIR.mkdir(exist_ok=True)
    path = DEBUG_DIR / f"{name}.html"
    path.write_text(html[:200_000], encoding="utf-8")
    log.info("  Debug saved → %s (%d bytes)", path, len(html))


def parse_amount(text) -> Optional[float]:
    if not text: return None
    c = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        v = float(c); return v if v > 0 else None
    except ValueError: return None


def norm_date(raw: str) -> str:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try: return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return raw.strip()


def name_variants(name: str):
    name = name.strip().upper()
    v = {name}
    if "," in name:
        p = [x.strip() for x in name.split(",", 1)]
        if len(p) == 2: v.add(f"{p[1]} {p[0]}")
    else:
        p = name.split()
        if len(p) >= 2:
            v.add(f"{p[-1]}, {' '.join(p[:-1])}")
            v.add(f"{p[-1]} {' '.join(p[:-1])}")
    return v


def score(record: dict, cutoff: datetime):
    flags, s = [], 30
    cat   = record.get("cat","")
    amt   = record.get("amount")
    owner = (record.get("owner") or "").upper()
    filed = record.get("filed","")
    if cat in ("LP","RELLP"):                    flags.append("Lis pendens")
    if cat == "NOFC":                            flags.append("Pre-foreclosure")
    if cat in ("JUD","CCJ","DRJUD"):             flags.append("Judgment lien")
    if cat in ("LNCORPTX","LNIRS","LNFED","TAXDEED"): flags.append("Tax lien")
    if cat == "LNMECH":                          flags.append("Mechanic lien")
    if cat == "PRO":                             flags.append("Probate / estate")
    if re.search(r"\b(LLC|CORP|INC|LTD|TRUST)\b", owner): flags.append("LLC / corp owner")
    try:
        if datetime.strptime(filed[:10], "%Y-%m-%d") >= cutoff:
            flags.append("New this week"); s += 5
    except Exception: pass
    s += len(flags) * 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags: s += 20
    if amt:
        if amt > 100000: s += 15
        elif amt > 50000: s += 10
    if record.get("prop_address"): s += 5
    return min(s, 100), flags


# ── ASP.NET form helper ───────────────────────────────────────────────────────

def get_aspnet_state(url: str) -> tuple[dict, str]:
    """Load a page and extract all hidden ASP.NET fields + raw HTML."""
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    fields = {}
    for inp in soup.find_all("input"):
        n = inp.get("name","")
        if n: fields[n] = inp.get("value","")
    return fields, r.text


def post_search(url: str, overrides: dict, debug_name: str) -> str:
    """
    1. GET page to collect ViewState / EventValidation.
    2. POST with overrides (date range, instrument type, submit button).
    3. Return response HTML.
    """
    fields, init_html = get_aspnet_state(url)
    save_debug(f"{debug_name}_init", init_html)

    # Find submit button
    soup = BeautifulSoup(init_html, "lxml")
    for btn in soup.find_all("input", type=lambda t: t and t.lower() in ("submit","button","image")):
        bname = btn.get("name","")
        bval  = btn.get("value","")
        if "search" in (bname+bval).lower() or btn.get("id","").lower().find("search") >= 0:
            fields[bname] = bval
            log.info("  Found search button: name=%s value=%s", bname, bval)
            break
    else:
        # fallback — add any button
        for btn in soup.find_all(["input","button"]):
            if btn.get("type","").lower() in ("submit","button"):
                n = btn.get("name","")
                if n: fields[n] = btn.get("value","")
                break

    # Apply caller overrides
    fields.update(overrides)

    log.info("  POSTing to %s with %d fields", url, len(fields))
    r = SESSION.post(url, data=fields, timeout=45,
                     headers={"Referer": url,
                               "Content-Type": "application/x-www-form-urlencoded",
                               "Origin": "https://www.cclerk.hctx.net"})
    r.raise_for_status()
    save_debug(f"{debug_name}_results", r.text)
    return r.text


# ── field name discovery ──────────────────────────────────────────────────────

def discover_fields(html: str) -> dict:
    """Return a map of semantic role → actual field name."""
    soup  = BeautifulSoup(html, "lxml")
    found = {}
    for inp in soup.find_all(["input","select"]):
        n = inp.get("name","")
        i = inp.get("id","")
        combined = (n + i).lower()
        if not found.get("date_from") and any(x in combined for x in
                ["datefrom","startdate","fromdate","begindate","datebegin"]):
            found["date_from"] = n
        if not found.get("date_to") and any(x in combined for x in
                ["dateto","enddate","todate","dateend","thrudate","throughdate"]):
            found["date_to"] = n
        if not found.get("instrument") and any(x in combined for x in
                ["instrumenttype","instrtype","instrument","doctype","doctypecode"]):
            found["instrument"] = n
    log.info("  Discovered fields: %s", found)
    return found


# ── HTML results parser ───────────────────────────────────────────────────────

def parse_results(html: str, cat: str, cat_label: str, base_url: str) -> list:
    soup    = BeautifulSoup(html, "lxml")
    records = []
    tables  = soup.find_all("table")
    log.info("  Response: %d tables found", len(tables))

    text_lower = soup.get_text(" ").lower()
    if any(x in text_lower for x in ["no records found","no results","0 records","no documents found"]):
        log.info("  Portal says: no records")
        return []

    for table in tables:
        rows    = table.find_all("tr")
        if len(rows) < 2: continue
        headers = [td.get_text(" ", strip=True).lower()
                   for td in rows[0].find_all(["th","td"])]
        joined  = " ".join(headers)
        if not any(k in joined for k in
                   ["file","instrument","grantor","date","type","case","party","deed","lien","name"]):
            continue
        log.info("  Parsing table — headers: %s", headers[:8])
        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells: continue
            try:
                data = {headers[i]: cells[i].get_text(" ", strip=True)
                        for i in range(min(len(headers), len(cells)))}
                link = None
                for cell in cells:
                    a = cell.find("a", href=True)
                    if a:
                        href = a["href"]
                        link = href if href.startswith("http") else \
                               f"https://www.cclerk.hctx.net{href}"
                        break

                def f(*keys):
                    for k in keys:
                        for h in headers:
                            if k in h:
                                v = data.get(h,"").strip()
                                if v: return v
                    return ""

                doc_num  = f("file number","instrument number","case number","file no","number")
                doc_type = f("instrument type","type","code","doc type")
                filed    = f("date filed","filed date","recording date","file date","date","recorded")
                grantor  = f("grantor","owner","debtor","party 1","applicant","name")
                grantee  = f("grantee","lender","creditor","party 2","secured party")
                legal    = f("legal","description","subdivision","property desc")
                amount   = f("amount","consideration","debt","balance","judgment")

                if not doc_num and not grantor: continue
                records.append({
                    "doc_num":  doc_num,  "doc_type": doc_type or cat_label,
                    "cat": cat, "cat_label": cat_label,
                    "filed":   norm_date(filed) if filed else "",
                    "owner":   grantor,  "grantee": grantee,
                    "amount":  parse_amount(amount), "legal": legal,
                    "clerk_url": link or "",
                    "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                })
            except Exception as e:
                log.debug("Row error: %s", e)
    log.info("  → %d records parsed", len(records))
    return records


# ── per-search functions ──────────────────────────────────────────────────────

def search_rp(start: str, end: str, code: str, cat: str, label: str) -> list:
    log.info("Searching RP: %s — %s", code, label)
    try:
        _, init_html = get_aspnet_state(RP)
        flds = discover_fields(init_html)
        overrides = {}
        if flds.get("date_from"):  overrides[flds["date_from"]]  = start
        if flds.get("date_to"):    overrides[flds["date_to"]]    = end
        if flds.get("instrument"): overrides[flds["instrument"]] = code
        # fallback field names if discovery missed them
        if not flds.get("date_from"):
            overrides.update({"ctl00$ContentPlaceHolder1$DateFrom": start,
                               "DateFrom": start, "txtDateFrom": start})
        if not flds.get("date_to"):
            overrides.update({"ctl00$ContentPlaceHolder1$DateTo": end,
                               "DateTo": end, "txtDateTo": end})
        if not flds.get("instrument"):
            overrides.update({"ctl00$ContentPlaceHolder1$InstrumentType": code,
                               "InstrumentType": code})
        html = post_search(RP, overrides, f"rp_{code.replace('/','_')}")
        return parse_results(html, cat, label, RP)
    except Exception as e:
        log.warning("RP search failed [%s]: %s", code, e)
        return []


def search_foreclosures(start: str, end: str) -> list:
    log.info("Searching foreclosures...")
    try:
        _, init_html = get_aspnet_state(FRCL)
        flds = discover_fields(init_html)
        overrides = {}
        if flds.get("date_from"): overrides[flds["date_from"]] = start
        if flds.get("date_to"):   overrides[flds["date_to"]]   = end
        if not flds.get("date_from"):
            overrides.update({"ctl00$ContentPlaceHolder1$DateFrom": start, "DateFrom": start})
        if not flds.get("date_to"):
            overrides.update({"ctl00$ContentPlaceHolder1$DateTo": end, "DateTo": end})
        html = post_search(FRCL, overrides, "foreclosures")
        return parse_results(html, "NOFC", "Notice of Foreclosure", FRCL)
    except Exception as e:
        log.warning("Foreclosure search failed: %s", e); return []


def search_probate(start: str, end: str) -> list:
    log.info("Searching probate...")
    try:
        _, init_html = get_aspnet_state(PROB)
        flds = discover_fields(init_html)
        overrides = {}
        if flds.get("date_from"): overrides[flds["date_from"]] = start
        if flds.get("date_to"):   overrides[flds["date_to"]]   = end
        if not flds.get("date_from"):
            overrides.update({"ctl00$ContentPlaceHolder1$DateFrom": start, "DateFrom": start})
        if not flds.get("date_to"):
            overrides.update({"ctl00$ContentPlaceHolder1$DateTo": end, "DateTo": end})
        html = post_search(PROB, overrides, "probate")
        return parse_results(html, "PRO", "Probate", PROB)
    except Exception as e:
        log.warning("Probate search failed: %s", e); return []


# ── parcel lookup ─────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self): self._d = {}
    def build(self, session):
        log.info("Building HCAD parcel lookup...")
        for year in [datetime.now().year, datetime.now().year-1]:
            for url in [f"https://pdata.hcad.org/GIS/Parcels/Parcels_{year}.zip",
                        f"https://pdata.hcad.org/data/{year}/parcels.zip"]:
                try:
                    r = session.get(url, timeout=120, stream=True)
                    if r.status_code == 200 and self._load_zip(r.content):
                        log.info("Parcel lookup: %d entries", len(self._d)); return
                except Exception: pass
        log.warning("Parcel data unavailable — no address enrichment")

    def lookup(self, owner):
        for v in name_variants(owner):
            if v in self._d: return self._d[v]
        return None

    def _load_zip(self, content):
        try: zf = zipfile.ZipFile(io.BytesIO(content))
        except Exception: return False
        for name in zf.namelist():
            lo = name.lower()
            if lo.endswith(".dbf") and HAS_DBF: return self._load_dbf(zf.read(name))
            if lo.endswith(".csv"): return self._load_csv(zf.read(name).decode("utf-8","replace"))
        return False

    def _load_dbf(self, data):
        p = Path("/tmp/_p.dbf"); p.write_bytes(data)
        try:
            for row in DBF(str(p), encoding="latin-1", ignore_missing_memofile=True):
                self._ingest(dict(row))
            return bool(self._d)
        except Exception as e: log.warning("DBF error: %s", e); return False

    def _load_csv(self, text):
        for row in csv.DictReader(io.StringIO(text)): self._ingest(row)
        return bool(self._d)

    def _ingest(self, row):
        def g(*ks):
            for k in ks:
                for v in (k, k.upper(), k.lower()):
                    val = row.get(v)
                    if val and str(val).strip(): return str(val).strip()
            return ""
        owner = g("OWNER","OWN1","OWNER1","OWNERNAME")
        if not owner: return
        info = {"prop_address": g("SITE_ADDR","SITEADDR"),
                "prop_city":    g("SITE_CITY","SITECITY"),
                "prop_state":   "TX",
                "prop_zip":     g("SITE_ZIP","SITEZIP"),
                "mail_address": g("ADDR_1","MAILADR1","MAIL_ADDR"),
                "mail_city":    g("CITY","MAILCITY"),
                "mail_state":   g("STATE","MAILSTATE") or "TX",
                "mail_zip":     g("ZIP","MAILZIP")}
        for v in name_variants(owner): self._d.setdefault(v, info)


# ── GHL CSV export ────────────────────────────────────────────────────────────

def export_csv(records, path):
    cols = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State",
            "Mailing Zip","Property Address","Property City","Property State","Property Zip",
            "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
            "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for r in records:
            p = (r.get("owner") or "").strip().split()
            amt = r.get("amount")
            w.writerow({
                "First Name": p[0].title() if p else "",
                "Last Name":  " ".join(p[1:]).title() if len(p)>1 else "",
                "Mailing Address": r.get("mail_address",""),
                "Mailing City":    r.get("mail_city",""),
                "Mailing State":   r.get("mail_state","TX"),
                "Mailing Zip":     r.get("mail_zip",""),
                "Property Address": r.get("prop_address",""),
                "Property City":    r.get("prop_city",""),
                "Property State":   r.get("prop_state","TX"),
                "Property Zip":     r.get("prop_zip",""),
                "Lead Type":        r.get("cat_label",""),
                "Document Type":    r.get("doc_type",""),
                "Date Filed":       r.get("filed",""),
                "Document Number":  r.get("doc_num",""),
                "Amount/Debt Owed": f"${amt:,.2f}" if amt else "",
                "Seller Score":     r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source": "Harris County Clerk",
                "Public Records URL": r.get("clerk_url",""),
            })
    log.info("GHL CSV → %s", path)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start  = cutoff.strftime("%m/%d/%Y")
    end    = now.strftime("%m/%d/%Y")

    log.info("=== Harris County Lead Scraper v4 ===")
    log.info("Range: %s → %s", start, end)

    parcel = ParcelLookup()
    parcel.build(SESSION)

    raw = []
    for code, cat, label in INSTRUMENT_TYPES:
        raw.extend(search_rp(start, end, code, cat, label))
        time.sleep(1)
    raw.extend(search_foreclosures(start, end))
    raw.extend(search_probate(start, end))

    # Deduplicate
    seen, deduped = set(), []
    for r in raw:
        key = r.get("doc_num") or f"{r.get('owner')}|{r.get('filed')}"
        if key and key not in seen:
            seen.add(key); deduped.append(r)

    log.info("Unique records: %d", len(deduped))

    with_addr = 0
    for r in deduped:
        try:
            addr = parcel.lookup(r.get("owner",""))
            if addr:
                r.update(addr)
                if r.get("prop_address"): with_addr += 1
            r["score"], r["flags"] = score(r, cutoff)
        except Exception as e:
            log.debug("Enrich error: %s", e)
            r.setdefault("score", 30); r.setdefault("flags", [])

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)

    payload = {
        "fetched_at":   now.isoformat(),
        "source":       "Harris County Clerk – cclerk.hctx.net",
        "date_range":   {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total":        len(deduped),
        "with_address": with_addr,
        "records":      deduped,
    }

    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        (d/"records.json").write_text(json.dumps(payload, indent=2, default=str))
        log.info("Saved → %s/records.json", d)

    export_csv(deduped, Path("data/leads_ghl.csv"))
    log.info("Done. total=%d with_address=%d", len(deduped), with_addr)


if __name__ == "__main__":
    main()
