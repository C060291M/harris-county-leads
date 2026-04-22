"""
Harris County Clerk FTP Scraper
Pulls daily index files from the Clerk's FTP server and processes:
  - Real Property (RP) records — LP, liens, judgments, tax deeds, etc.
  - Assumed Names (ASN) — DBA filings (bonus leads: LLC owners)
  - Probate (PRO) — estate/probate filings
  - Foreclosures (FRCL) — trustee sale notices

FTP Structure (per ReadMe):
  /IndexData/
    YYYYMMDD_RPSubscriber.zip   → RPInstruments.txt, RPOwners.txt, RPGrantees.txt
    YYYYMMDD_ASNSubscriber.zip  → ASNInstruments.txt, ASNOwners.txt, ASNDBAs.txt
    YYYYMMDD_PROSubscriber.zip  → PROInstruments.txt, PROOwners.txt
    YYYYMMDD_FRCLSubscriber.zip → FRCLInstruments.txt

All files: pipe-delimited (|), first row = header.

Config:
  Set FTP_HOST, FTP_USER, FTP_PASS as GitHub Secrets (env vars).
  Falls back to scraping cclerk.hctx.net if FTP not configured.
"""

import csv, io, json, logging, os, re, sys, time, zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

OUTPUT_DIRS   = [Path("dashboard"), Path("data")]
DEBUG_DIR     = Path("debug")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

# FTP credentials from environment / GitHub Secrets
FTP_HOST = os.environ.get("FTP_HOST", "")
FTP_USER = os.environ.get("FTP_USER", "")
FTP_PASS = os.environ.get("FTP_PASS", "")

# ── Instrument type → category mapping ───────────────────────────────────────
# Real Property instrument codes (from Codes.aspx?DTI=1)
RP_CAT_MAP = {
    "LP":       ("LP",       "Lis Pendens"),
    "RELLP":    ("RELLP",    "Release Lis Pendens"),
    "A/J":      ("JUD",      "Abstract of Judgment"),
    "CCJ":      ("CCJ",      "Certified Judgment"),
    "DRJUD":    ("DRJUD",    "Domestic Relations Judgment"),
    "LNIRS":    ("LNIRS",    "IRS Lien"),
    "LNFED":    ("LNFED",    "Federal Lien"),
    "LNCORPTX": ("LNCORPTX", "Corp Tax Lien"),
    "LN":       ("LN",       "Lien"),
    "LNMECH":   ("LNMECH",   "Mechanic Lien"),
    "LNHOA":    ("LNHOA",    "HOA Lien"),
    "MEDLN":    ("MEDLN",    "Medicaid Lien"),
    "TAXDEED":  ("TAXDEED",  "Tax Deed"),
    "NOC":      ("NOC",      "Notice of Commencement"),
    "NOFC":     ("NOFC",     "Notice of Foreclosure"),
    "PRO":      ("PRO",      "Probate"),
}

TARGET_CATS = set(RP_CAT_MAP.keys())


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_amount(text) -> Optional[float]:
    if not text: return None
    c = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        v = float(c); return v if v > 0 else None
    except ValueError: return None


def norm_date(raw: str) -> str:
    """Handle YYYYMMDD (FTP format) and MM/DD/YYYY (portal format)."""
    raw = raw.strip()
    if re.match(r"^\d{8}$", raw):
        try: return datetime.strptime(raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError: pass
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try: return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return raw


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


def business_days_back(n: int) -> list[str]:
    """Return list of YYYYMMDD strings for last n business days."""
    dates = []
    d = datetime.now()
    while len(dates) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:  # Mon–Fri
            dates.append(d.strftime("%Y%m%d"))
    return dates


def compute_score(record: dict, cutoff: datetime):
    flags, s = [], 10
    cat   = record.get("cat", "")
    amt   = record.get("amount")
    owner = (record.get("owner") or "").upper()
    filed = record.get("filed", "")
    legal = (record.get("legal") or "").upper()
    dtype = (record.get("doc_type") or "").upper()

    # Distress signals
    if cat in ("TAXDEED", "LNIRS", "LNCORPTX", "LNFED"):
        flags.append("Tax delinquency"); s += 30
    if cat in ("LNMECH", "LNHOA"):
        flags.append("Code / HOA violation"); s += 25
    if cat == "PRO":
        flags.append("Probate / estate"); s += 20
    if cat in {"LN","LNMECH","LNHOA","LNIRS","LNFED","LNCORPTX","MEDLN"}:
        if "Lien on record" not in flags:
            flags.append("Lien on record"); s += 15
    if any(k in legal+dtype for k in ["DIVORCE","DISSOLUTION","BANKRUPTCY"]) or cat=="DRJUD":
        flags.append("Divorce / bankruptcy"); s += 10
    if cat in ("LP","RELLP"):
        flags.append("Lis pendens"); s += 10
    if cat == "NOFC":
        flags.append("Pre-foreclosure"); s += 10
    if cat in ("JUD","CCJ"):
        flags.append("Judgment lien"); s += 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        s += 20
    if re.search(r"\b(LLC|CORP|INC|LTD|TRUST)\b", owner):
        flags.append("LLC / corp owner")
    try:
        if datetime.strptime(filed[:10], "%Y-%m-%d") >= cutoff:
            flags.append("New this week"); s += 5
    except Exception: pass
    if amt:
        if amt > 100000: s += 15
        elif amt > 50000: s += 10
    if record.get("prop_address"): s += 5
    s += len(flags) * 2
    return min(s, 100), flags


def blank_record(doc_num, doc_type, cat, cat_label, filed, owner,
                 grantee="", amount=None, legal="", clerk_url=""):
    return {
        "doc_num": doc_num, "doc_type": doc_type,
        "cat": cat, "cat_label": cat_label,
        "filed": filed, "owner": owner, "grantee": grantee,
        "amount": amount, "legal": legal, "clerk_url": clerk_url,
        "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
        "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
    }


# ── SFTP downloader (Harris County uses SFTP at sftp.cclerk.hctx.net) ──────────

class FTPClient:
    """
    SFTP client for Harris County Clerk FTP server.
    Server: sftp.cclerk.hctx.net (SFTP / SSH protocol, port 22)
    Files are in /IndexData/ folder.
    """
    def __init__(self, host, user, password):
        self.host     = host
        self.user     = user
        self.password = password
        self._ssh     = None
        self._sftp    = None
        self._home    = f"/users/{user}"

    def connect(self):
        import paramiko
        log.info("Connecting to SFTP: %s", self.host)
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(
            hostname = self.host,
            port     = 22,
            username = self.user,
            password = self.password,
            timeout  = 30,
            banner_timeout = 30,
            auth_timeout   = 30,
        )
        self._sftp = self._ssh.open_sftp()
        # Get home directory to build correct paths
        try:
            self._home = self._sftp.getcwd() or f"/users/{user}"
            if not self._home or self._home == "/":
                self._home = f"/users/{user}"
        except Exception:
            self._home = f"/users/{user}"
        log.info("SFTP connected to %s, home: %s", self.host, self._home)

    def disconnect(self):
        try:
            if self._sftp: self._sftp.close()
            if self._ssh:  self._ssh.close()
        except Exception: pass

    def list_files(self, path="/IndexData") -> list[str]:
        try:
            return self._sftp.listdir(path)
        except Exception as e:
            log.warning("SFTP list error at %s: %s", path, e)
            return []

    def download_zip(self, filename: str, path="/IndexData") -> Optional[bytes]:
        full_path = f"{path}/{filename}"
        buf = io.BytesIO()
        try:
            self._sftp.getfo(full_path, buf)
            size = buf.tell()
            log.info("Downloaded %s (%d bytes)", filename, size)
            buf.seek(0)
            return buf.read()
        except FileNotFoundError:
            log.debug("File not found on SFTP: %s", full_path)
            return None
        except Exception as e:
            log.warning("SFTP download failed %s: %s", filename, e)
            return None

    def get_daily_zips(self, date_str: str, record_type: str) -> Optional[bytes]:
        """
        Harris County SFTP folder/filename mapping (confirmed from web interface):
          Real Property:  /users/<user>/Index_RP/YYYYMMDD_RPISubscriber.zip
          Probate:        /users/<user>/Index_PRO/YYYYMMDD_PROSubscriber.zip
          Foreclosure:    /users/<user>/Index_FRCL/YYYYMMDD_FRCLSubscriber.zip
          Assumed Names:  /users/<user>/Index_ASN/YYYYMMDD_ASNSubscriber.zip
        """
        folder_map = {
            "RP":   ("Index_RP",   f"{date_str}_RPISubscriber.zip"),
            "PRO":  ("Index_PRO",  f"{date_str}_PROSubscriber.zip"),
            "FRCL": ("Index_FRCL", f"{date_str}_FRCLSubscriber.zip"),
            "ASN":  ("Index_ASN",  f"{date_str}_ASNSubscriber.zip"),
        }
        if record_type not in folder_map:
            log.warning("Unknown record type: %s", record_type)
            return None
        folder, filename = folder_map[record_type]
        full_path = f"{self._home}/{folder}"
        log.info("Looking for %s in %s", filename, full_path)
        return self.download_zip(filename, path=full_path)


# ── FTP zip parsers ───────────────────────────────────────────────────────────

def parse_rp_zip(zip_bytes: bytes, date_str: str) -> list[dict]:
    """
    Parse Real Property zip.
    Files: RPInstruments.txt, RPGrantors.txt (or RPOwners.txt), RPGrantees.txt
    Columns in RPInstruments.txt (pipe-delimited):
      File Number | Film Code | File Date | Instrument Type |
      No. of Pages | Consideration Amount | Legal Description |
      Subdivision | Vol | Page | ...
    """
    records = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad RP zip %s: %s", date_str, e)
        return []

    names = {n.lower(): n for n in zf.namelist()}
    log.info("RP zip %s contains: %s", date_str, list(zf.namelist()))

    # Find instrument file
    instr_key = next((k for k in names if "instrument" in k), None)
    owner_key  = next((k for k in names if "grantor" in k or "owner" in k), None)
    grantee_key = next((k for k in names if "grantee" in k), None)

    if not instr_key:
        log.warning("No instrument file in RP zip %s", date_str)
        return []

    # Parse instruments
    instr_rows = {}
    instr_text = zf.read(names[instr_key]).decode("latin-1", errors="replace")
    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        fn = row.get("File Number","").strip()
        if fn:
            instr_rows[fn] = row

    # Parse owners/grantors
    owner_rows = {}
    if owner_key:
        owner_text = zf.read(names[owner_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(owner_text), delimiter="|"):
            fn = row.get("File Number","").strip()
            if fn:
                owner_rows.setdefault(fn, []).append(row)

    # Parse grantees
    grantee_rows = {}
    if grantee_key:
        grantee_text = zf.read(names[grantee_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(grantee_text), delimiter="|"):
            fn = row.get("File Number","").strip()
            if fn:
                grantee_rows.setdefault(fn, []).append(row)

    # Build records
    for fn, instr in instr_rows.items():
        # Get instrument type — try multiple field name variants
        itype = (instr.get("Instrument Type") or
                 instr.get("Instrument Code") or
                 instr.get("Doc Type") or "").strip().upper()

        cat, cat_label = RP_CAT_MAP.get(itype, (None, None))
        if not cat:
            # Try partial match
            for code, (c, l) in RP_CAT_MAP.items():
                if code in itype:
                    cat, cat_label = c, l
                    break
        if not cat:
            continue  # Not a target document type

        filed = norm_date(instr.get("File Date", date_str))
        amount = parse_amount(instr.get("Consideration Amount") or
                              instr.get("Amount") or "")
        legal = (instr.get("Legal Description") or
                 instr.get("Subdivision") or "").strip()
        film = instr.get("Film Code Number","").strip()
        clerk_url = f"https://www.cclerk.hctx.net/Applications/WebSearch/RP_R.aspx?FilmCode={film}" if film else ""

        owners = owner_rows.get(fn, [])
        grantees = grantee_rows.get(fn, [])

        owner_name = " / ".join(
            o.get("Owner Name", o.get("Grantor Name","")).strip()
            for o in owners if o.get("Owner Name", o.get("Grantor Name","")).strip()
        ) or ""

        grantee_name = " / ".join(
            g.get("Grantee Name","").strip()
            for g in grantees if g.get("Grantee Name","").strip()
        ) or ""

        rec = blank_record(fn, itype, cat, cat_label, filed,
                           owner_name, grantee_name, amount, legal, clerk_url)

        # Enrich with owner address if available
        if owners:
            o = owners[0]
            rec["mail_address"] = o.get("Address","").strip()
            rec["mail_city"]    = o.get("City","").strip()
            rec["mail_state"]   = o.get("State","TX").strip() or "TX"
            rec["mail_zip"]     = o.get("Zip","").strip()

        records.append(rec)

    log.info("RP %s: %d target records (from %d instruments)", date_str, len(records), len(instr_rows))
    return records


def parse_pro_zip(zip_bytes: bytes, date_str: str) -> list[dict]:
    """Parse Probate zip."""
    records = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad PRO zip %s: %s", date_str, e); return []

    names = {n.lower(): n for n in zf.namelist()}
    instr_key = next((k for k in names if "instrument" in k), None)
    owner_key  = next((k for k in names if "owner" in k or "party" in k), None)
    if not instr_key: return []

    instr_text = zf.read(names[instr_key]).decode("latin-1", errors="replace")
    owner_rows = {}
    if owner_key:
        owner_text = zf.read(names[owner_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(owner_text), delimiter="|"):
            fn = row.get("File Number","").strip()
            if fn: owner_rows.setdefault(fn, []).append(row)

    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        fn    = row.get("File Number","").strip()
        filed = norm_date(row.get("File Date", date_str))
        film  = row.get("Film Code Number","").strip()
        clerk_url = f"https://www.cclerk.hctx.net/Applications/WebSearch/PRO_R.aspx?FilmCode={film}" if film else ""

        owners = owner_rows.get(fn, [])
        owner_name = " / ".join(
            o.get("Owner Name", o.get("Party Name","")).strip()
            for o in owners if (o.get("Owner Name") or o.get("Party Name","")).strip()
        )
        rec = blank_record(fn, "PRO", "PRO", "Probate", filed, owner_name,
                           clerk_url=clerk_url)
        if owners:
            o = owners[0]
            rec["mail_address"] = o.get("Address","").strip()
            rec["mail_city"]    = o.get("City","").strip()
            rec["mail_state"]   = o.get("State","TX").strip() or "TX"
            rec["mail_zip"]     = o.get("Zip","").strip()
        records.append(rec)

    log.info("PRO %s: %d records", date_str, len(records))
    return records


def parse_frcl_zip(zip_bytes: bytes, date_str: str) -> list[dict]:
    """Parse Foreclosure zip."""
    records = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad FRCL zip %s: %s", date_str, e); return []

    names = {n.lower(): n for n in zf.namelist()}
    instr_key = next((k for k in names if "instrument" in k or "frcl" in k), None)
    if not instr_key: return []

    instr_text = zf.read(names[instr_key]).decode("latin-1", errors="replace")
    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        fn    = row.get("File Number","").strip()
        filed = norm_date(row.get("File Date") or row.get("Sale Date", date_str))
        owner = (row.get("Grantor Name") or row.get("Debtor","")).strip()
        grantee = (row.get("Grantee Name") or row.get("Trustee","")).strip()
        amount = parse_amount(row.get("Amount") or row.get("Consideration",""))
        legal  = row.get("Legal Description","").strip()
        film   = row.get("Film Code Number","").strip()
        clerk_url = f"https://www.cclerk.hctx.net/Applications/WebSearch/FRCL_R.aspx?FilmCode={film}" if film else ""
        rec = blank_record(fn, "NOFC", "NOFC", "Notice of Foreclosure",
                           filed, owner, grantee, amount, legal, clerk_url)
        records.append(rec)

    log.info("FRCL %s: %d records", date_str, len(records))
    return records


def parse_asn_zip(zip_bytes: bytes, date_str: str) -> list[dict]:
    """
    Parse Assumed Names zip for LLC/Corp owner leads.
    Active filings by incorporated entities are motivated seller signals.
    """
    records = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad ASN zip %s: %s", date_str, e); return []

    names_map = {n.lower(): n for n in zf.namelist()}
    instr_key  = next((k for k in names_map if "instrument" in k), None)
    owner_key  = next((k for k in names_map if "owner" in k), None)
    dba_key    = next((k for k in names_map if "dba" in k), None)
    if not instr_key: return []

    instr_text = zf.read(names_map[instr_key]).decode("latin-1", errors="replace")
    owner_rows = {}
    dba_rows   = {}

    if owner_key:
        owner_text = zf.read(names_map[owner_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(owner_text), delimiter="|"):
            fn = row.get("File Number","").strip()
            if fn: owner_rows.setdefault(fn, []).append(row)

    if dba_key:
        dba_text = zf.read(names_map[dba_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(dba_text), delimiter="|"):
            fn = row.get("File Number","").strip()
            if fn: dba_rows[fn] = row

    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        # Only active (not withdrawn)
        if row.get("Assumed Name or Withdrawn","").strip().upper() == "W":
            continue

        fn    = row.get("File Number","").strip()
        filed = norm_date(row.get("File Date", date_str))
        film  = row.get("Film Code Number","").strip()
        clerk_url = f"https://www.cclerk.hctx.net/Applications/WebSearch/ASN_R.aspx?FilmCode={film}" if film else ""

        owners = owner_rows.get(fn, [])
        dba    = dba_rows.get(fn, {})

        owner_name = " / ".join(
            o.get("Owner Name","").strip() for o in owners if o.get("Owner Name","").strip()
        )
        dba_name = dba.get("DBA Name","").strip()

        rec = blank_record(fn, "ASN", "ASN", f"Assumed Name: {dba_name}",
                           filed, owner_name, clerk_url=clerk_url)
        rec["legal"] = dba_name

        if owners:
            o = owners[0]
            rec["mail_address"] = o.get("Address","").strip()
            rec["mail_city"]    = o.get("City","").strip()
            rec["mail_state"]   = o.get("State","TX").strip() or "TX"
            rec["mail_zip"]     = o.get("Zip","").strip()
            rec["prop_address"] = dba.get("Address","").strip()
            rec["prop_city"]    = dba.get("City","").strip()
            rec["prop_state"]   = "TX"
            rec["prop_zip"]     = dba.get("Zip","").strip()

        records.append(rec)

    log.info("ASN %s: %d active records", date_str, len(records))
    return records


# ── Fallback: scrape portal (used when FTP not configured) ────────────────────

def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def portal_search_rp(start: str, end: str, code: str, cat: str, label: str) -> list[dict]:
    from urllib.parse import unquote
    session = make_session()
    RP = "https://www.cclerk.hctx.net/Applications/WebSearch/RP.aspx"
    btn = "ctl00$ContentPlaceHolder1$btnSearch"
    try:
        r = session.get(RP, timeout=30); r.raise_for_status()
        soup = BeautifulSoup(r.text,"lxml")
        vs = {i.get("name"):i.get("value","") for i in soup.find_all("input",type="hidden") if i.get("name")}
        payload = {**vs,
            "ctl00$ContentPlaceHolder1$txtFrom": start,
            "ctl00$ContentPlaceHolder1$txtTo":   end,
            "ctl00$ContentPlaceHolder1$txtInstrument": code,
            "ctl00$ScriptManager1": f"ctl00$ScriptManager1|{btn}",
            "__ASYNCPOST":"true","__EVENTTARGET":"","__EVENTARGUMENT":"",
            btn:"Search"}
        r2 = session.post(RP, data=payload, timeout=45, headers={
            "Referer":RP,"Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
            "X-MicrosoftAjax":"Delta=true","X-Requested-With":"XMLHttpRequest",
            "Origin":"https://www.cclerk.hctx.net","Accept":"*/*"})
        delta = r2.text
        m = re.search(r'pageRedirect\|\|([^|]+)', delta)
        if not m: return []
        redir = "https://www.cclerk.hctx.net" + unquote(m.group(1))
        rp = session.get(redir, timeout=30)
        return _parse_portal_table(rp.text, cat, label)
    except Exception as e:
        log.warning("Portal search failed [%s]: %s", code, e); return []


def _parse_portal_table(html: str, cat: str, cat_label: str) -> list[dict]:
    soup = BeautifulSoup(html,"lxml")
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows)<2: continue
        headers = [td.get_text(" ",strip=True).lower() for td in rows[0].find_all(["th","td"])]
        if not any(k in " ".join(headers) for k in ["file","grantor","date","names"]): continue
        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells: continue
            data = {headers[i]: cells[i].get_text(" ",strip=True) for i in range(min(len(headers),len(cells)))}
            link = next((("https://www.cclerk.hctx.net"+a["href"] if not a["href"].startswith("http") else a["href"])
                         for cell in cells for a in cell.find_all("a",href=True)), "")
            def f(*k):
                for key in k:
                    for h in headers:
                        if key in h:
                            v=data.get(h,"").strip()
                            if v: return v
                return ""
            doc_num = f("file number","number","case number")
            grantor = f("names","grantor","owner","debtor","name")
            filed   = f("file date","date filed","date")
            if not doc_num and not grantor: continue
            records.append(blank_record(doc_num, cat_label, cat, cat_label,
                           norm_date(filed) if filed else "", grantor, clerk_url=link))
    return records


# ── GHL CSV export ────────────────────────────────────────────────────────────

def export_csv(records: list, path: Path):
    cols = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State",
            "Mailing Zip","Property Address","Property City","Property State","Property Zip",
            "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
            "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f,fieldnames=cols); w.writeheader()
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

    log.info("=== Harris County FTP Scraper ===")
    log.info("FTP configured: %s", bool(FTP_HOST and FTP_USER and FTP_PASS))
    log.info("Lookback: %d days", LOOKBACK_DAYS)

    raw = []

    if FTP_HOST and FTP_USER and FTP_PASS:
        # ── FTP MODE: fast, complete, reliable ──
        ftp = FTPClient(FTP_HOST, FTP_USER, FTP_PASS)
        try:
            ftp.connect()

            dates = business_days_back(LOOKBACK_DAYS)
            log.info("Fetching %d business days: %s … %s", len(dates), dates[-1], dates[0])

            for date_str in dates:
                # Real Property
                log.info("Fetching RP %s", date_str)
                rp_zip = ftp.get_daily_zips(date_str, "RP")
                if rp_zip:
                    raw.extend(parse_rp_zip(rp_zip, date_str))
                else:
                    log.info("  No RP file for %s (non-business day or not yet posted)", date_str)

                # Probate
                pro_zip = ftp.get_daily_zips(date_str, "PRO")
                if pro_zip:
                    raw.extend(parse_pro_zip(pro_zip, date_str))

                # Foreclosures
                frcl_zip = ftp.get_daily_zips(date_str, "FRCL")
                if frcl_zip:
                    raw.extend(parse_frcl_zip(frcl_zip, date_str))

                # Assumed Names (bonus LLC leads)
                asn_zip = ftp.get_daily_zips(date_str, "ASN")
                if asn_zip:
                    raw.extend(parse_asn_zip(asn_zip, date_str))

                time.sleep(0.5)  # be polite to FTP

        finally:
            ftp.disconnect()

    else:
        # ── PORTAL FALLBACK MODE ──
        log.warning("FTP credentials not set — falling back to portal scraping")
        log.warning("Set FTP_HOST, FTP_USER, FTP_PASS as GitHub Secrets for full data")
        start = cutoff.strftime("%m/%d/%Y")
        end   = now.strftime("%m/%d/%Y")
        PORTAL_TYPES = [
            ("LP","LP","Lis Pendens"),("RELLP","RELLP","Release Lis Pendens"),
            ("A/J","JUD","Abstract of Judgment"),("CCJ","CCJ","Certified Judgment"),
            ("LNIRS","LNIRS","IRS Lien"),("LNFED","LNFED","Federal Lien"),
            ("LNCORPTX","LNCORPTX","Corp Tax Lien"),("LN","LN","Lien"),
            ("LNMECH","LNMECH","Mechanic Lien"),("LNHOA","LNHOA","HOA Lien"),
            ("MEDLN","MEDLN","Medicaid Lien"),("TAXDEED","TAXDEED","Tax Deed"),
            ("NOC","NOC","Notice of Commencement"),
        ]
        for code, cat, label in PORTAL_TYPES:
            raw.extend(portal_search_rp(start, end, code, cat, label))
            time.sleep(1)

    # Deduplicate
    seen, deduped = set(), []
    for r in raw:
        key = r.get("doc_num") or f"{r.get('owner')}|{r.get('filed')}"
        if key and key not in seen:
            seen.add(key); deduped.append(r)

    log.info("Unique records: %d", len(deduped))

    # Score
    with_addr = 0
    for r in deduped:
        try:
            r["score"], r["flags"] = compute_score(r, cutoff)
            if r.get("prop_address") or r.get("mail_address"):
                with_addr += 1
        except Exception as e:
            log.debug("Score error: %s", e)
            r.setdefault("score",10); r.setdefault("flags",[])

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)

    payload = {
        "fetched_at":   now.isoformat(),
        "source":       "Harris County Clerk FTP",
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
