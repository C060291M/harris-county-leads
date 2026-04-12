"""
Harris County Motivated Seller Lead Scraper v7
FINAL - Correctly handles ASP.NET UpdatePanel redirect pattern.

Flow discovered from debug analysis:
  1. GET RP.aspx → collect ViewState
  2. POST with UpdatePanel AJAX headers + search fields
  3. Delta response contains: pageRedirect||/RP_R.aspx?ID=<session_token>
  4. GET RP_R.aspx?ID=<token> → parse results table
  5. Paginate via Next button on results page

Same pattern for Probate (CourtSearch_R.aspx).
Foreclosures use a different inline calendar approach.
"""
import csv, io, json, logging, re, sys, time, zipfile
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import unquote
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

BASE   = "https://www.cclerk.hctx.net/Applications/WebSearch"
RP     = f"{BASE}/RP.aspx"
FRCL   = f"{BASE}/FRCL_R.aspx"
PROB   = f"{BASE}/CourtSearch.aspx?CaseType=Probate"
RP_R   = f"{BASE}/RP_R.aspx"
PROB_R = f"{BASE}/CourtSearch_R.aspx"

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
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
})


# ── helpers ───────────────────────────────────────────────────────────────────

def save_debug(name, content):
    DEBUG_DIR.mkdir(exist_ok=True)
    p = DEBUG_DIR / f"{name}.txt"
    data = content if isinstance(content, str) else content.decode("utf-8","replace")
    p.write_text(data[:300_000], encoding="utf-8")


def parse_amount(text):
    if not text: return None
    c = re.sub(r"[^\d.]", "", str(text).replace(",",""))
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


# ── core HTTP functions ───────────────────────────────────────────────────────

def get_viewstate(url: str) -> dict:
    """GET page and return all hidden ASP.NET form fields."""
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    fields = {}
    for inp in soup.find_all("input", type="hidden"):
        n = inp.get("name","")
        if n: fields[n] = inp.get("value","")
    return fields


def updatepanel_search(url: str, search_fields: dict, debug_name: str) -> Optional[str]:
    """
    Submit UpdatePanel AJAX search.
    Returns the redirect URL from the Delta response, or None.
    """
    vs = get_viewstate(url)
    btn = "ctl00$ContentPlaceHolder1$btnSearch"

    payload = {
        **vs,
        **search_fields,
        "ctl00$ScriptManager1": f"ctl00$ScriptManager1|{btn}",
        "__ASYNCPOST":          "true",
        "__EVENTTARGET":        "",
        "__EVENTARGUMENT":      "",
        btn:                    "Search",
    }

    r = SESSION.post(url, data=payload, timeout=45, headers={
        "Referer":           url,
        "Content-Type":      "application/x-www-form-urlencoded; charset=UTF-8",
        "X-MicrosoftAjax":  "Delta=true",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":            "https://www.cclerk.hctx.net",
        "Accept":            "*/*",
    })
    r.raise_for_status()

    delta = r.text
    save_debug(debug_name, delta)
    log.info("  Delta: %d bytes", len(delta))

    # Extract pageRedirect URL
    m = re.search(r'pageRedirect\|\|([^|]+)', delta)
    if m:
        rel = unquote(m.group(1))
        full = "https://www.cclerk.hctx.net" + rel
        log.info("  Redirect → %s...", full[:80])
        return full

    log.warning("  No pageRedirect found in Delta for %s", debug_name)
    return None


def fetch_results_page(url: str, debug_name: str) -> str:
    """GET a results page and return its HTML."""
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    save_debug(f"{debug_name}_results", r.text)
    return r.text


def get_next_page_url(html: str, base_url: str) -> Optional[str]:
    """Find the 'Next' pagination link in results page."""
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        if text in ("next", "next >", ">>", "next page"):
            href = a["href"]
            if href.startswith("http"): return href
            if href.startswith("/"): return "https://www.cclerk.hctx.net" + href
            return base_url.rsplit("/",1)[0] + "/" + href
    return None


# ── results table parser ──────────────────────────────────────────────────────

def parse_results(html: str, cat: str, cat_label: str) -> list:
    soup    = BeautifulSoup(html, "lxml")
    records = []
    tables  = soup.find_all("table")
    log.info("  Tables: %d", len(tables))

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2: continue
        headers = [td.get_text(" ", strip=True).lower()
                   for td in rows[0].find_all(["th","td"])]
        joined  = " ".join(headers)
        if not any(k in joined for k in ["file","instrument","grantor","date","type",
                                          "case","party","deed","lien","name","sale"]):
            continue
        log.info("  Parsing table: %s (%d rows)", headers[:5], len(rows)-1)

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
                               "https://www.cclerk.hctx.net" + href
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
                grantee  = f("grantee","lender","creditor","party 2","secured")
                legal    = f("legal","description","subdivision")
                amount   = f("amount","consideration","debt","balance","judgment")

                if not doc_num and not grantor: continue
                records.append({
                    "doc_num":  doc_num,
                    "doc_type": doc_type or cat_label,
                    "cat":      cat, "cat_label": cat_label,
                    "filed":    norm_date(filed) if filed else "",
                    "owner":    grantor, "grantee": grantee,
                    "amount":   parse_amount(amount), "legal": legal,
                    "clerk_url": link or "",
                    "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                })
            except Exception as e:
                log.debug("Row: %s", e)

    log.info("  → %d records", len(records))
    return records


# ── search functions ──────────────────────────────────────────────────────────

def search_rp(start: str, end: str, code: str, cat: str, label: str) -> list:
    log.info("RP: %s (%s)", code, label)
    try:
        redirect = updatepanel_search(RP, {
            "ctl00$ContentPlaceHolder1$txtFrom":       start,
            "ctl00$ContentPlaceHolder1$txtTo":         end,
            "ctl00$ContentPlaceHolder1$txtInstrument": code,
        }, f"rp_{code.replace('/','_')}")

        if not redirect:
            return []

        all_recs = []
        page_num = 0
        url = redirect
        while url and page_num < 20:
            page_num += 1
            html = fetch_results_page(url, f"rp_{code.replace('/','_')}_p{page_num}")
            recs = parse_results(html, cat, label)
            all_recs.extend(recs)
            log.info("  Page %d: %d records", page_num, len(recs))
            if not recs: break
            url = get_next_page_url(html, url)

        return all_recs
    except Exception as e:
        log.warning("RP failed [%s]: %s", code, e)
        return []


def search_foreclosures(start: str, end: str) -> list:
    """Foreclosure page uses year/month dropdowns."""
    log.info("Foreclosures...")
    all_recs = []
    now = datetime.now()
    months = set()
    for d in range(LOOKBACK_DAYS + 1):
        dt = now - timedelta(days=d)
        months.add((str(dt.year), str(dt.month)))

    for year, month in sorted(months):
        log.info("  FRCL %s/%s", month, year)
        try:
            # Foreclosures don't redirect — they use inline rendering
            # We search by year+month and parse the results table directly
            vs = get_viewstate(FRCL)
            btn = "ctl00$ContentPlaceHolder1$btnSearch"
            payload = {
                **vs,
                "ctl00$ContentPlaceHolder1$ddlYear":  year,
                "ctl00$ContentPlaceHolder1$ddlMonth": month,
                "ctl00$ContentPlaceHolder1$rbtlDate": "FileDate",
                "ctl00$ScriptManager1": f"ctl00$ScriptManager1|{btn}",
                "__ASYNCPOST": "true",
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                btn: "Search",
            }
            r = SESSION.post(FRCL, data=payload, timeout=45, headers={
                "Referer": FRCL,
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-MicrosoftAjax": "Delta=true",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "https://www.cclerk.hctx.net",
                "Accept": "*/*",
            })
            r.raise_for_status()
            delta = r.text
            save_debug(f"frcl_{year}_{month}", delta)

            # Check if there's a redirect
            m = re.search(r'pageRedirect\|\|([^|]+)', delta)
            if m:
                redir = "https://www.cclerk.hctx.net" + unquote(m.group(1))
                html = fetch_results_page(redir, f"frcl_{year}_{month}_r")
                recs = parse_results(html, "NOFC", "Notice of Foreclosure")
            else:
                # Parse inline — look for table in the full page
                full_r = SESSION.get(FRCL, timeout=30)
                recs = parse_results(full_r.text, "NOFC", "Notice of Foreclosure")

            all_recs.extend(recs)
            log.info("  FRCL %s/%s: %d records", month, year, len(recs))
        except Exception as e:
            log.warning("FRCL %s/%s failed: %s", month, year, e)

    log.info("Foreclosures total: %d", len(all_recs))
    return all_recs


def search_probate(start: str, end: str) -> list:
    log.info("Probate...")
    try:
        redirect = updatepanel_search(PROB, {
            "ctl00$ContentPlaceHolder1$txtFrom2":           start,
            "ctl00$ContentPlaceHolder1$txtTo2":             end,
            "ctl00$ContentPlaceHolder1$ddlCourt":           "All",
            "ctl00$ContentPlaceHolder1$DropDownListStatus": "-All",
        }, "probate")

        if not redirect:
            return []

        all_recs = []
        page_num = 0
        url = redirect
        while url and page_num < 20:
            page_num += 1
            html = fetch_results_page(url, f"probate_p{page_num}")
            recs = parse_results(html, "PRO", "Probate")
            all_recs.extend(recs)
            if not recs: break
            url = get_next_page_url(html, url)

        log.info("Probate total: %d", len(all_recs))
        return all_recs
    except Exception as e:
        log.warning("Probate failed: %s", e)
        return []


# ── parcel lookup ─────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self): self._d = {}

    def build(self, session):
        log.info("Building HCAD parcel lookup...")
        year = datetime.now().year
        loaded = False

        for yr in [year, year - 1]:
            if loaded: break
            for url in [
                f"https://pdata.hcad.org/data/{yr}/Real_acct_owner.zip",
                f"https://pdata.hcad.org/CAMA/{yr}/Real_acct_owner.zip",
                f"https://pdata.hcad.org/data/{yr}/real_acct_owner.zip",
            ]:
                try:
                    log.info("  Trying parcel URL: %s", url)
                    r = session.get(url, timeout=120, stream=True)
                    if r.status_code == 200:
                        log.info("  Downloaded %d bytes", len(r.content))
                        loaded = self._load_zip(r.content)
                        if loaded: break
                except Exception as e:
                    log.debug("  URL failed: %s", e)

        if not loaded:
            for yr in [year, year - 1]:
                if loaded: break
                for url in [
                    f"https://pdata.hcad.org/GIS/Parcels/Parcels_{yr}.zip",
                    f"https://pdata.hcad.org/data/{yr}/parcels.zip",
                ]:
                    try:
                        r = session.get(url, timeout=120, stream=True)
                        if r.status_code == 200:
                            loaded = self._load_zip(r.content)
                            if loaded: break
                    except Exception: pass

        if loaded:
            log.info("Parcel lookup ready: %d entries", len(self._d))
        else:
            log.warning("Parcel data unavailable")

    def lookup(self, owner):
        if not owner: return None
        for v in name_variants(owner):
            if v in self._d: return self._d[v]
        return None

    def _load_zip(self, content):
        try:
            zf = zipfile.ZipFile(io.BytesIO(content))
        except Exception:
            return False
        names = zf.namelist()
        log.info("  Zip contents: %s", names[:10])
        for name in names:
            lo = name.lower()
            if "real_acct" in lo and lo.endswith(".txt"):
                return self._load_tab(zf.read(name).decode("latin-1", errors="replace"))
            if lo.endswith(".dbf") and HAS_DBF:
                return self._load_dbf(zf.read(name))
            if lo.endswith(".csv"):
                return self._load_csv(zf.read(name).decode("utf-8", errors="replace"))
        return False

    def _load_tab(self, text):
        """Parse HCAD tab-delimited real_acct.txt.
        Columns: acct, yr, owner_name, owner_name_2, owner_address,
                 owner_city, owner_state, owner_zipcode,
                 site_addr_1, site_addr_2, site_addr_3
        """
        lines = text.splitlines()
        if not lines: return False
        first = lines[0].lower()
        start = 1 if ("acct" in first or "owner" in first) else 0
        count = 0
        for line in lines[start:]:
            parts = line.split("\t")
            if len(parts) < 5: continue
            try:
                owner = parts[2].strip()
                if not owner: continue
                info = {
                    "prop_address": parts[8].strip()  if len(parts) > 8  else "",
                    "prop_city":    parts[9].strip()  if len(parts) > 9  else "HOUSTON",
                    "prop_state":   "TX",
                    "prop_zip":     parts[10].strip() if len(parts) > 10 else "",
                    "mail_address": parts[4].strip()  if len(parts) > 4  else "",
                    "mail_city":    parts[5].strip()  if len(parts) > 5  else "",
                    "mail_state":   parts[6].strip()  if len(parts) > 6  else "TX",
                    "mail_zip":     parts[7].strip()  if len(parts) > 7  else "",
                }
                for v in name_variants(owner):
                    self._d.setdefault(v, info)
                count += 1
            except Exception:
                pass
        log.info("  Parsed %d tab records", count)
        return count > 0

    def _load_dbf(self, data):
        p = Path("/tmp/_p.dbf"); p.write_bytes(data)
        try:
            for row in DBF(str(p), encoding="latin-1", ignore_missing_memofile=True):
                self._ingest_row(dict(row))
            return bool(self._d)
        except Exception as e:
            log.warning("DBF: %s", e); return False

    def _load_csv(self, text):
        for row in csv.DictReader(io.StringIO(text)): self._ingest_row(row)
        return bool(self._d)

    def _ingest_row(self, row):
        def g(*ks):
            for k in ks:
                for var in (k, k.upper(), k.lower()):
                    v = row.get(var)
                    if v and str(v).strip(): return str(v).strip()
            return ""
        owner = g("OWNER_NAME","OWNER","OWN1","OWNER1","OWNERNAME")
        if not owner: return
        info = {
            "prop_address": g("SITE_ADDR_1","SITE_ADDR","SITEADDR"),
            "prop_city":    g("SITE_ADDR_3","SITE_CITY","SITECITY") or "HOUSTON",
            "prop_state":   "TX",
            "prop_zip":     g("SITE_ZIP","SITEZIP"),
            "mail_address": g("OWNER_ADDRESS","ADDR_1","MAILADR1"),
            "mail_city":    g("OWNER_CITY","CITY","MAILCITY"),
            "mail_state":   g("OWNER_STATE","STATE","MAILSTATE") or "TX",
            "mail_zip":     g("OWNER_ZIPCODE","ZIP","MAILZIP"),
        }
        for v in name_variants(owner):
            self._d.setdefault(v, info)


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

    log.info("=== Harris County Lead Scraper v7 ===")
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
            r["score"], r["flags"] = compute_score(r, cutoff)
        except Exception as e:
            log.debug("Enrich: %s", e)
            r.setdefault("score",30); r.setdefault("flags",[])

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
