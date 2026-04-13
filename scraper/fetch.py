"""
Harris County Motivated Seller Lead Scraper v8
Fixes:
1. Fresh session per RP search (fixes shared session token bug)
2. HCAD address lookup via public.hcad.org API (fixes bulk download fail)
3. Better results table parser for A/J style tables
"""
import csv, io, json, logging, re, sys, time, zipfile
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import unquote, quote
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


def make_session():
    """Create a fresh requests session."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return s


def save_debug(name, content):
    DEBUG_DIR.mkdir(exist_ok=True)
    p = DEBUG_DIR / f"{name}.txt"
    data = content if isinstance(content, str) else str(content)
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


# ── Core search: fresh session per call ──────────────────────────────────────

def get_viewstate(session, url):
    r = session.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    fields = {}
    for inp in soup.find_all("input", type="hidden"):
        n = inp.get("name","")
        if n: fields[n] = inp.get("value","")
    return fields


def updatepanel_post(url, search_fields, debug_name):
    """
    Use a FRESH session for each search to avoid shared session tokens.
    Returns redirect URL from Delta response.
    """
    session = make_session()  # fresh session every time!
    btn = "ctl00$ContentPlaceHolder1$btnSearch"

    vs = get_viewstate(session, url)
    payload = {
        **vs,
        **search_fields,
        "ctl00$ScriptManager1": f"ctl00$ScriptManager1|{btn}",
        "__ASYNCPOST":          "true",
        "__EVENTTARGET":        "",
        "__EVENTARGUMENT":      "",
        btn:                    "Search",
    }

    r = session.post(url, data=payload, timeout=45, headers={
        "Referer":           url,
        "Content-Type":      "application/x-www-form-urlencoded; charset=UTF-8",
        "X-MicrosoftAjax":  "Delta=true",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":            "https://www.cclerk.hctx.net",
        "Accept":            "*/*",
    })
    r.raise_for_status()
    delta = r.text
    save_debug(debug_name, delta[:5000])
    log.info("  Delta: %d bytes", len(delta))

    m = re.search(r'pageRedirect\|\|([^|]+)', delta)
    if m:
        rel  = unquote(m.group(1))
        full = "https://www.cclerk.hctx.net" + rel
        log.info("  Redirect → %s...", full[:80])
        # IMPORTANT: use same session to follow redirect (session cookie required)
        return session, full

    log.warning("  No pageRedirect in Delta for %s", debug_name)
    return session, None


def fetch_results(session, url, debug_name):
    r = session.get(url, timeout=30)
    r.raise_for_status()
    save_debug(f"{debug_name}_r", r.text[:50000])
    return r.text


def get_next_page(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        t = a.get_text(strip=True).lower()
        if t in ("next", "next >", ">>", "next page", ">"):
            href = a["href"]
            if href.startswith("http"): return href
            if href.startswith("/"): return "https://www.cclerk.hctx.net" + href
            return base_url.rsplit("/",1)[0] + "/" + href
    return None


# ── Results parser ────────────────────────────────────────────────────────────

def parse_results(html, cat, cat_label):
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
                                          "case","party","deed","lien","name","sale",
                                          "recorded","names"]):
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

                doc_num  = f("file number","instrument number","case number",
                              "file no","number","doc no","file")
                doc_type = f("instrument type","type vol page","type","code","doc type")
                filed    = f("file date","date filed","filed date","recording date","date")
                grantor  = f("names","grantor","owner","debtor","party 1","applicant","name")
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


# ── Search functions ──────────────────────────────────────────────────────────

def search_rp(start, end, code, cat, label):
    log.info("RP: %s (%s)", code, label)
    try:
        session, redirect = updatepanel_post(RP, {
            "ctl00$ContentPlaceHolder1$txtFrom":       start,
            "ctl00$ContentPlaceHolder1$txtTo":         end,
            "ctl00$ContentPlaceHolder1$txtInstrument": code,
        }, f"rp_{code.replace('/','_')}")

        if not redirect: return []

        all_recs = []
        url = redirect
        page_num = 0
        while url and page_num < 20:
            page_num += 1
            html = fetch_results(session, url, f"rp_{code.replace('/','_')}_p{page_num}")
            recs = parse_results(html, cat, label)
            all_recs.extend(recs)
            log.info("  Page %d: %d records", page_num, len(recs))
            if not recs: break
            url = get_next_page(html, url)
        return all_recs
    except Exception as e:
        log.warning("RP failed [%s]: %s", code, e)
        return []


def search_foreclosures(start, end):
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
            session, redirect = updatepanel_post(FRCL, {
                "ctl00$ContentPlaceHolder1$ddlYear":   year,
                "ctl00$ContentPlaceHolder1$ddlMonth":  month,
                "ctl00$ContentPlaceHolder1$rbtlDate":  "FileDate",
            }, f"frcl_{year}_{month}")

            if redirect:
                html = fetch_results(session, redirect, f"frcl_{year}_{month}_r")
                recs = parse_results(html, "NOFC", "Notice of Foreclosure")
                all_recs.extend(recs)
                log.info("  FRCL %s/%s: %d records", month, year, len(recs))
        except Exception as e:
            log.warning("FRCL %s/%s: %s", month, year, e)

    log.info("Foreclosures total: %d", len(all_recs))
    return all_recs


def search_probate(start, end):
    log.info("Probate...")
    try:
        session, redirect = updatepanel_post(PROB, {
            "ctl00$ContentPlaceHolder1$txtFrom2":              start,
            "ctl00$ContentPlaceHolder1$txtTo2":                end,
            "ctl00$ContentPlaceHolder1$ddlCourt":              "All",
            "ctl00$ContentPlaceHolder1$DropDownListStatus":    "-All",
        }, "probate")

        if not redirect: return []

        all_recs = []
        url = redirect
        page_num = 0
        while url and page_num < 20:
            page_num += 1
            html = fetch_results(session, url, f"probate_p{page_num}")
            recs = parse_results(html, "PRO", "Probate")
            all_recs.extend(recs)
            if not recs: break
            url = get_next_page(html, url)

        log.info("Probate total: %d", len(all_recs))
        return all_recs
    except Exception as e:
        log.warning("Probate: %s", e)
        return []


# ── HCAD address lookup via public search API ─────────────────────────────────

class ParcelLookup:
    """
    Looks up property addresses via HCAD's public record search.
    Uses batch name searches against public.hcad.org.
    Caches results to avoid redundant requests.
    """
    def __init__(self):
        self._cache = {}   # owner_name → {prop_address, ...}
        self._session = make_session()

    def build(self, session):
        """No bulk download needed — we query on-demand."""
        log.info("HCAD lookup ready (on-demand via public.hcad.org)")

    def lookup(self, owner):
        if not owner or len(owner.strip()) < 3:
            return None
        owner = owner.strip().upper()

        # Check cache first
        if owner in self._cache:
            return self._cache[owner]

        # Try each name variant
        for variant in name_variants(owner):
            result = self._search(variant)
            if result:
                # Cache all variants
                for v in name_variants(owner):
                    self._cache[v] = result
                return result

        self._cache[owner] = None
        return None

    def _search(self, name):
        """Query HCAD public search for owner name."""
        try:
            url = "https://public.hcad.org/records/Real/Advanced.asp"
            params = {
                "crypt": "",
                "ownr": name,
                "stype": "A",
                "taxyear": str(datetime.now().year),
            }
            r = self._session.get(url, params=params, timeout=15)
            if r.status_code != 200:
                return None

            soup = BeautifulSoup(r.text, "lxml")
            # Results table has columns: Account, Owner, Site Address, Mailing Address
            for table in soup.find_all("table"):
                rows = table.find_all("tr")
                if len(rows) < 2: continue
                headers = [td.get_text(" ",strip=True).lower()
                           for td in rows[0].find_all(["th","td"])]
                if not any(k in " ".join(headers) for k in ["owner","site","mail","account"]):
                    continue
                for row in rows[1:2]:  # just first result
                    cells = row.find_all("td")
                    if len(cells) < 3: continue
                    data = {headers[i]: cells[i].get_text(" ",strip=True)
                            for i in range(min(len(headers),len(cells)))}

                    def g(*ks):
                        for k in ks:
                            for h in headers:
                                if k in h:
                                    v = data.get(h,"").strip()
                                    if v: return v
                        return ""

                    site  = g("site address","site addr","situs")
                    mail  = g("mailing address","mail addr","mail")
                    city  = g("city","site city")
                    zipcd = g("zip","postal")

                    if site or mail:
                        return {
                            "prop_address": site,
                            "prop_city":    city or "HOUSTON",
                            "prop_state":   "TX",
                            "prop_zip":     zipcd,
                            "mail_address": mail or site,
                            "mail_city":    city or "HOUSTON",
                            "mail_state":   "TX",
                            "mail_zip":     zipcd,
                        }
        except Exception as e:
            log.debug("HCAD lookup error for %s: %s", name, e)
        return None


# ── GHL CSV ───────────────────────────────────────────────────────────────────

def export_csv(records, path):
    cols = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State",
            "Mailing Zip","Property Address","Property City","Property State","Property Zip",
            "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
            "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path,"w",newline="",encoding="utf-8") as f:
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start  = cutoff.strftime("%m/%d/%Y")
    end    = now.strftime("%m/%d/%Y")

    log.info("=== Harris County Lead Scraper v8 ===")
    log.info("Range: %s → %s", start, end)

    parcel = ParcelLookup()
    parcel.build(None)

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

    # Enrich with addresses (on-demand HCAD lookup)
    with_addr = 0
    for i, r in enumerate(deduped):
        try:
            addr = parcel.lookup(r.get("owner",""))
            if addr:
                r.update(addr)
                if r.get("prop_address"): with_addr += 1
            r["score"], r["flags"] = compute_score(r, cutoff)
        except Exception as e:
            log.debug("Enrich: %s", e)
            r.setdefault("score",30); r.setdefault("flags",[])

        if (i+1) % 50 == 0:
            log.info("  Enriched %d/%d (with_addr=%d)", i+1, len(deduped), with_addr)

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
