"""
Harris County Motivated Seller Lead Scraper v6
Uses ASP.NET UpdatePanel AJAX POST with exact field names.

Key insight from debug analysis:
- The page uses Sys.WebForms.PageRequestManager (UpdatePanel)
- Regular POST returns blank form; must use UpdatePanel AJAX headers:
    ScriptManager: ctl00$ScriptManager1|ctl00$ContentPlaceHolder1$btnSearch
    __ASYNCPOST: true
    X-MicrosoftAjax: Delta=true
    X-Requested-With: XMLHttpRequest
- Response is a Delta (partial page update), not full HTML
- Results are in the Delta payload between |updatePanel| markers

Real Property field names (confirmed from debug HTML):
  txtFrom      = Date From
  txtTo        = Date To
  txtInstrument = Instrument Type code
  btnSearch    = submit

Foreclosure field names:
  ddlYear      = year dropdown
  ddlMonth     = month dropdown
  btnSearch    = submit

Probate field names:
  txtFrom2     = Date From (case filed date)
  txtTo2       = Date To
  btnSearch    = submit
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
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
})


# ── helpers ───────────────────────────────────────────────────────────────────

def save_debug(name, content):
    DEBUG_DIR.mkdir(exist_ok=True)
    p = DEBUG_DIR / f"{name}.txt"
    if isinstance(content, bytes):
        p.write_bytes(content[:300_000])
    else:
        p.write_text(str(content)[:300_000], encoding="utf-8")


def parse_amount(text):
    if not text: return None
    c = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        v = float(c); return v if v > 0 else None
    except ValueError: return None


def norm_date(raw):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try: return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return raw.strip()


def name_variants(name):
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


def compute_score(record, cutoff):
    flags, s = [], 30
    cat   = record.get("cat","")
    amt   = record.get("amount")
    owner = (record.get("owner") or "").upper()
    filed = record.get("filed","")
    if cat in ("LP","RELLP"):                          flags.append("Lis pendens")
    if cat == "NOFC":                                  flags.append("Pre-foreclosure")
    if cat in ("JUD","CCJ","DRJUD"):                   flags.append("Judgment lien")
    if cat in ("LNCORPTX","LNIRS","LNFED","TAXDEED"):  flags.append("Tax lien")
    if cat == "LNMECH":                                flags.append("Mechanic lien")
    if cat == "PRO":                                   flags.append("Probate / estate")
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


# ── ASP.NET UpdatePanel POST ──────────────────────────────────────────────────

def get_viewstate(url: str) -> dict:
    """Load page and collect hidden ASP.NET fields."""
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    fields = {}
    for inp in soup.find_all("input", type="hidden"):
        n = inp.get("name","")
        if n: fields[n] = inp.get("value","")
    return fields


def updatepanel_post(url: str, form_fields: dict, 
                     script_manager_target: str, debug_name: str) -> str:
    """
    Submit an ASP.NET UpdatePanel search.
    Returns extracted HTML from the Delta response.
    """
    # Step 1: get fresh ViewState
    vs = get_viewstate(url)

    # Step 2: merge base fields + our search fields
    payload = {**vs, **form_fields}
    payload["ctl00$ScriptManager1"] = (
        f"ctl00$ScriptManager1|{script_manager_target}"
    )
    payload["__ASYNCPOST"]    = "true"
    payload["__EVENTTARGET"]  = ""
    payload["__EVENTARGUMENT"] = ""

    headers = {
        "Referer":            url,
        "Content-Type":       "application/x-www-form-urlencoded; charset=UTF-8",
        "X-MicrosoftAjax":   "Delta=true",
        "X-Requested-With":  "XMLHttpRequest",
        "Origin":             "https://www.cclerk.hctx.net",
    }

    log.info("  UpdatePanel POST → %s", url.split("/")[-1])
    r = SESSION.post(url, data=payload, headers=headers, timeout=45)
    r.raise_for_status()

    raw = r.text
    save_debug(debug_name, raw)
    log.info("  Response: %d bytes", len(raw))

    # Step 3: extract HTML from Delta response
    # Delta format: length|type|id|content|
    # We want updatePanel sections and scriptBlock sections
    html_parts = []

    # Pattern: digits|updatePanel|id|content|
    for match in re.finditer(
        r'\d+\|updatePanel\|[^|]+\|(.*?)(?=\d+\|(?:updatePanel|hiddenField|scriptBlock|pageTitle|asyncPostBackControlIDs|postBackControlIDs|updatePanelIDs|asyncPostBackTimeout|formAction|focus)|$)',
        raw, re.DOTALL
    ):
        html_parts.append(match.group(1))

    if html_parts:
        combined = "\n".join(html_parts)
        log.info("  Extracted %d HTML parts from Delta (%d chars)", 
                 len(html_parts), len(combined))
        return combined

    # Fallback: if not Delta format (maybe full page), return as-is
    log.info("  Not a Delta response — using full response")
    return raw


# ── results table parser ──────────────────────────────────────────────────────

def parse_results(html: str, cat: str, cat_label: str, base_url: str) -> list:
    soup    = BeautifulSoup(html, "lxml")
    records = []
    tables  = soup.find_all("table")
    log.info("  Tables found: %d", len(tables))

    page_text = soup.get_text(" ").lower()
    if any(x in page_text for x in ["no records found","no results","0 records",
                                      "no documents found","nothing found"]):
        log.info("  Portal: no records for this query")
        return []

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2: continue
        headers = [td.get_text(" ", strip=True).lower()
                   for td in rows[0].find_all(["th","td"])]
        joined  = " ".join(headers)
        if not any(k in joined for k in ["file","instrument","grantor","date",
                                          "type","case","party","deed","lien",
                                          "name","sale","recorded"]):
            continue
        log.info("  Table headers: %s", headers[:8])

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

                doc_num  = f("file number","instrument number","case number",
                              "file no","number","doc no")
                doc_type = f("instrument type","type","code","doc type")
                filed    = f("date filed","filed date","recording date",
                              "file date","date","recorded","sale date")
                grantor  = f("grantor","owner","debtor","party 1","applicant","name")
                grantee  = f("grantee","lender","creditor","party 2","secured")
                legal    = f("legal","description","subdivision","property desc")
                amount   = f("amount","consideration","debt","balance","judgment")

                if not doc_num and not grantor: continue
                records.append({
                    "doc_num":  doc_num,
                    "doc_type": doc_type or cat_label,
                    "cat":      cat,
                    "cat_label": cat_label,
                    "filed":    norm_date(filed) if filed else "",
                    "owner":    grantor,
                    "grantee":  grantee,
                    "amount":   parse_amount(amount),
                    "legal":    legal,
                    "clerk_url": link or "",
                    "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                })
            except Exception as e:
                log.debug("Row error: %s", e)

    log.info("  → %d records parsed", len(records))
    return records


# ── search functions ──────────────────────────────────────────────────────────

def search_rp(start: str, end: str, code: str, cat: str, label: str) -> list:
    log.info("RP: %s (%s)", code, label)
    try:
        html = updatepanel_post(
            url=RP,
            form_fields={
                "ctl00$ContentPlaceHolder1$txtFrom":       start,
                "ctl00$ContentPlaceHolder1$txtTo":         end,
                "ctl00$ContentPlaceHolder1$txtInstrument": code,
                "ctl00$ContentPlaceHolder1$btnSearch":     "Search",
            },
            script_manager_target="ctl00$ContentPlaceHolder1$btnSearch",
            debug_name=f"rp_{code.replace('/','_')}",
        )
        return parse_results(html, cat, label, RP)
    except Exception as e:
        log.warning("RP failed [%s]: %s", code, e)
        return []


def search_foreclosures(start: str, end: str) -> list:
    log.info("Foreclosures (by month)...")
    all_recs = []
    now = datetime.now()
    months = set()
    for d in range(LOOKBACK_DAYS + 1):
        dt = now - timedelta(days=d)
        months.add((str(dt.year), str(dt.month)))

    for year, month in sorted(months):
        log.info("  FRCL %s/%s", month, year)
        try:
            html = updatepanel_post(
                url=FRCL,
                form_fields={
                    "ctl00$ContentPlaceHolder1$ddlYear":   year,
                    "ctl00$ContentPlaceHolder1$ddlMonth":  month,
                    "ctl00$ContentPlaceHolder1$rbtlDate":  "SaleDate",
                    "ctl00$ContentPlaceHolder1$btnSearch": "Search",
                },
                script_manager_target="ctl00$ContentPlaceHolder1$btnSearch",
                debug_name=f"frcl_{year}_{month}",
            )
            recs = parse_results(html, "NOFC", "Notice of Foreclosure", FRCL)
            all_recs.extend(recs)
        except Exception as e:
            log.warning("FRCL failed %s/%s: %s", month, year, e)

    log.info("Foreclosures total: %d", len(all_recs))
    return all_recs


def search_probate(start: str, end: str) -> list:
    log.info("Probate...")
    try:
        html = updatepanel_post(
            url=PROB,
            form_fields={
                "ctl00$ContentPlaceHolder1$txtFrom2":              start,
                "ctl00$ContentPlaceHolder1$txtTo2":                end,
                "ctl00$ContentPlaceHolder1$ddlCourt":              "All",
                "ctl00$ContentPlaceHolder1$DropDownListStatus":    "-All",
                "ctl00$ContentPlaceHolder1$btnSearch":             "Search",
            },
            script_manager_target="ctl00$ContentPlaceHolder1$btnSearch",
            debug_name="probate",
        )
        recs = parse_results(html, "PRO", "Probate", PROB)
        log.info("Probate: %d", len(recs))
        return recs
    except Exception as e:
        log.warning("Probate failed: %s", e)
        return []


# ── parcel lookup ─────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self): self._d = {}

    def build(self, session):
        log.info("Building HCAD parcel lookup...")
        for year in [datetime.now().year, datetime.now().year - 1]:
            for url in [
                f"https://pdata.hcad.org/GIS/Parcels/Parcels_{year}.zip",
                f"https://pdata.hcad.org/data/{year}/parcels.zip",
            ]:
                try:
                    r = session.get(url, timeout=120, stream=True)
                    if r.status_code == 200 and self._load_zip(r.content):
                        log.info("Parcels: %d entries", len(self._d)); return
                except Exception: pass
        log.warning("Parcel data unavailable")

    def lookup(self, owner):
        for v in name_variants(owner):
            if v in self._d: return self._d[v]
        return None

    def _load_zip(self, content):
        try: zf = zipfile.ZipFile(io.BytesIO(content))
        except Exception: return False
        for n in zf.namelist():
            lo = n.lower()
            if lo.endswith(".dbf") and HAS_DBF: return self._load_dbf(zf.read(n))
            if lo.endswith(".csv"): return self._load_csv(zf.read(n).decode("utf-8","replace"))
        return False

    def _load_dbf(self, data):
        p = Path("/tmp/_p.dbf"); p.write_bytes(data)
        try:
            for row in DBF(str(p), encoding="latin-1", ignore_missing_memofile=True):
                self._ingest(dict(row))
            return bool(self._d)
        except Exception as e: log.warning("DBF: %s", e); return False

    def _load_csv(self, text):
        for row in csv.DictReader(io.StringIO(text)): self._ingest(row)
        return bool(self._d)

    def _ingest(self, row):
        def g(*ks):
            for k in ks:
                for var in (k, k.upper(), k.lower()):
                    v = row.get(var)
                    if v and str(v).strip(): return str(v).strip()
            return ""
        owner = g("OWNER","OWN1","OWNER1","OWNERNAME")
        if not owner: return
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
        for v in name_variants(owner): self._d.setdefault(v, info)


# ── GHL CSV ───────────────────────────────────────────────────────────────────

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
                "First Name":  p[0].title() if p else "",
                "Last Name":   " ".join(p[1:]).title() if len(p)>1 else "",
                "Mailing Address": r.get("mail_address",""),
                "Mailing City":    r.get("mail_city",""),
                "Mailing State":   r.get("mail_state","TX"),
                "Mailing Zip":     r.get("mail_zip",""),
                "Property Address": r.get("prop_address",""),
                "Property City":    r.get("prop_city",""),
                "Property State":   r.get("prop_state","TX"),
                "Property Zip":     r.get("prop_zip",""),
                "Lead Type":    r.get("cat_label",""),
                "Document Type": r.get("doc_type",""),
                "Date Filed":    r.get("filed",""),
                "Document Number": r.get("doc_num",""),
                "Amount/Debt Owed": f"${amt:,.2f}" if amt else "",
                "Seller Score": r.get("score",0),
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

    log.info("=== Harris County Lead Scraper v6 ===")
    log.info("Range: %s → %s", start, end)

    parcel = ParcelLookup()
    parcel.build(SESSION)

    raw = []
    for code, cat, label in INSTRUMENT_TYPES:
        raw.extend(search_rp(start, end, code, cat, label))
        time.sleep(1.5)
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
            r["score"], r["flags"] = compute_score(r, cutoff)
        except Exception as e:
            log.debug("Enrich: %s", e)
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
