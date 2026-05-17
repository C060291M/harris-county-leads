"""
Harris County Clerk SFTP Scraper v10 - Production with HCAD Address Enrichment
SFTP: sftp.cclerk.hctx.net  Folder: /users/<user>/Index_RP/
HCAD: downloads Real_acct_owner.zip from pdata.hcad.org at runtime
"""
import csv, io, json, logging, os, re, sys, time, zipfile, gzip
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

OUTPUT_DIRS   = [Path("dashboard"), Path("data")]
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))
FTP_HOST = os.environ.get("FTP_HOST", "")
FTP_USER = os.environ.get("FTP_USER", "")
FTP_PASS = os.environ.get("FTP_PASS", "")

# HCAD bulk data URLs to try
HCAD_URLS = [
    f"https://pdata.hcad.org/data/cama/{datetime.now().year}/Real_acct_owner.zip",
    f"https://pdata.hcad.org/data/cama/{datetime.now().year - 1}/Real_acct_owner.zip",
    f"https://pdata.hcad.org/CAMA/{datetime.now().year}/Real_acct_owner.zip",
]

RP_CAT_MAP = {
    "LP":                    ("LP",       "Lis Pendens"),
    "LIS PENDENS":           ("LP",       "Lis Pendens"),
    "LIS PEN":               ("LP",       "Lis Pendens"),
    "RELLP":                 ("RELLP",    "Release Lis Pendens"),
    "REL LIS PEN":           ("RELLP",    "Release Lis Pendens"),
    "REL":                   ("RELLP",    "Release Lis Pendens"),
    "A/J":                   ("JUD",      "Abstract of Judgment"),
    "ABST OF JUD":           ("JUD",      "Abstract of Judgment"),
    "CCJ":                   ("CCJ",      "Certified Judgment"),
    "CERT JUDG":             ("CCJ",      "Certified Judgment"),
    "DRJUD":                 ("DRJUD",    "Domestic Relations Judgment"),
    "DOM REL JUD":           ("DRJUD",    "Domestic Relations Judgment"),
    "LNIRS":                 ("LNIRS",    "IRS Lien"),
    "IRS LIEN":              ("LNIRS",    "IRS Lien"),
    "FED TAX LIEN":          ("LNIRS",    "IRS Lien"),
    "LNFED":                 ("LNFED",    "Federal Lien"),
    "FED LIEN":              ("LNFED",    "Federal Lien"),
    "LNCORPTX":              ("LNCORPTX", "Corp Tax Lien"),
    "CORP TAX":              ("LNCORPTX", "Corp Tax Lien"),
    "STATE TAX LIEN":        ("LNCORPTX", "Corp Tax Lien"),
    "LN":                    ("LN",       "Lien"),
    "LIEN":                  ("LN",       "Lien"),
    "LNMECH":                ("LNMECH",   "Mechanic Lien"),
    "MECH LIEN":             ("LNMECH",   "Mechanic Lien"),
    "MECHANIC":              ("LNMECH",   "Mechanic Lien"),
    "LNHOA":                 ("LNHOA",    "HOA Lien"),
    "HOA LIEN":              ("LNHOA",    "HOA Lien"),
    "MEDLN":                 ("MEDLN",    "Medicaid Lien"),
    "MEDICAID":              ("MEDLN",    "Medicaid Lien"),
    "TAXDEED":               ("TAXDEED",  "Tax Deed"),
    "TAX DEED":              ("TAXDEED",  "Tax Deed"),
    "DEED":                  ("TAXDEED",  "Tax Deed"),
    "NOC":                   ("NOC",      "Notice of Commencement"),
    "NOTICE OF COM":         ("NOC",      "Notice of Commencement"),
    "NOFC":                  ("NOFC",     "Notice of Foreclosure"),
    "FORECLOSURE":           ("NOFC",     "Notice of Foreclosure"),
    "NOTICE OF FORECLOSURE": ("NOFC",     "Notice of Foreclosure"),
    "PRO":                   ("PRO",      "Probate"),
    "PROBATE":               ("PRO",      "Probate"),
    "QCD":                   ("LP",       "Quit Claim Deed"),
    "QUIT CLAIM":            ("LP",       "Quit Claim Deed"),
    "ASSGN":                 ("LN",       "Assignment of Lien"),
    "ASSIGNMENT":            ("LN",       "Assignment of Lien"),
}

FORECLOSURE_WORDS = {"TRUSTEE","MORTGAGE","BANK","LENDER","FINANCIAL",
                     "CREDIT","LOAN","MTGE","SUBSTITUTE","NATIONAL ASSOC",
                     "N.A.","FEDERAL","HOME LOAN","SERVICER","SERVICING"}


def parse_amount(text) -> Optional[float]:
    if not text: return None
    c = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        v = float(c); return v if v > 0 else None
    except ValueError: return None


def norm_date(raw: str) -> str:
    raw = raw.strip()
    if re.match(r"^\d{8}$", raw):
        try: return datetime.strptime(raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError: pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try: return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return raw


def business_days_back(n: int) -> list:
    dates, d = [], datetime.now()
    while len(dates) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            dates.append(d.strftime("%Y%m%d"))
    return dates


def classify(raw_type: str, grantee: str = "") -> tuple:
    t = raw_type.strip().upper()
    cat, label = RP_CAT_MAP.get(t, (None, None))
    if cat == "LP" and t == "NOTICE":
        grantee_up = grantee.upper()
        if any(w in grantee_up for w in FORECLOSURE_WORDS):
            return "NOFC", "Notice of Foreclosure"
    if cat: return cat, label
    for key, val in RP_CAT_MAP.items():
        if key and t and (key in t or t in key):
            return val
    return None, None



_ENTITY_KEYWORDS = [
    " LLC"," INC"," CORP"," LP"," LTD"," TRUST"," BANK"," MORTGAGE",
    " SERVICING"," FINANCIAL"," CAPITAL"," CREDIT"," AUTHORITY",
    " DEPARTMENT"," DISTRICT"," ASSOCIATION"," MANAGEMENT"," SERVICES",
    " SERVICE"," SOLUTIONS"," SYSTEMS","TRUSTEE"," TITLE"," INSURANCE",
    " COLLEGE"," SCHOOL","ATTORNEY","LAW OFFICE"," FOUNDATION",
    " HOLDINGS","FEDERAL "," AGENCY","COUNTY","STATE OF","CITY OF",
    " GOVERNMENT","NATIONAL ","REVENUE SERVICE","COMPTROLLER","AUCTION"
]
_HIGH_FREQ: set = set()

def _is_person(name: str) -> bool:
    if not name: return False
    upper = name.upper()
    if any(k in upper for k in _ENTITY_KEYWORDS): return False
    return len(upper.split()) >= 2

def _extract_primary(owner_raw: str, high_freq: set) -> str:
    if not owner_raw: return ""
    parts = [p.strip() for p in owner_raw.split("/") if p.strip()]
    for part in parts:
        upper = part.upper()
        if upper in high_freq: continue
        if any(k in upper for k in _ENTITY_KEYWORDS): continue
        if len(upper.split()) >= 2: return part.strip()
    return parts[0].strip() if parts else ""

def compute_score(record: dict, cutoff: datetime) -> tuple:
    flags, s = [], 10
    cat   = record.get("cat", "")
    amt   = record.get("amount")
    owner = (record.get("owner") or "").upper()
    filed = record.get("filed", "")
    legal = (record.get("legal") or "").upper()
    dtype = (record.get("doc_type") or "").upper()

    if cat in ("TAXDEED","LNIRS","LNCORPTX","LNFED"):
        flags.append("Tax delinquency"); s += 30
    if cat in ("LNMECH","LNHOA"):
        flags.append("Code / HOA violation"); s += 25
    if cat == "PRO":
        flags.append("Probate / estate"); s += 20
    if cat in {"LN","LNMECH","LNHOA","LNIRS","LNFED","LNCORPTX","MEDLN"}:
        if "Lien on record" not in flags:
            flags.append("Lien on record"); s += 15
    if any(k in legal+dtype for k in ["DIVORCE","DISSOLUTION","BANKRUPTCY"]) or cat=="DRJUD":
        if "Divorce / bankruptcy" not in flags:
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
    if record.get("prop_address") or record.get("mail_address"):
        s += 5
    # Absentee owner bonus — mail state differs from TX property
    mail_state = (record.get("mail_state") or "").upper().strip()
    prop_state = (record.get("prop_state") or "TX").upper().strip()
    if mail_state and mail_state != "TX":
        flags.append("Absentee owner (out of state)"); s += 15
    # Address + lien combo — highly motivated
    has_address = bool(record.get("prop_address") or record.get("mail_address"))
    has_lien = any("Lien" in f or "Tax" in f for f in flags)
    if has_address and has_lien:
        s += 10
    # Multiple distress signals
    if len(flags) >= 3:
        s += 10
    s += len(flags) * 2
    return min(s, 100), flags


def blank_rec(fn, dtype, cat, cat_label, filed, owner,
              grantee="", amount=None, legal="", url=""):
    return {
        "doc_num": fn, "doc_type": dtype, "cat": cat, "cat_label": cat_label,
        "filed": filed, "owner": owner, "grantee": grantee,
        "amount": amount, "legal": legal, "clerk_url": url,
        "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
        "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
    }


# ── HCAD Address Lookup ───────────────────────────────────────────────────────

class HCADLookup:
    def __init__(self):
        self._lookup = {}
        self._prefix = {}

    def build(self):
        log.info("Building HCAD address lookup...")

        # Try local file first (committed to repo — fastest, always works)
        # Support split files (_1/_2) and single file
        local_paths = [
            ["data/hcad_lookup_1.json.gz", "data/hcad_lookup_2.json.gz"],
            ["data/hcad_lookup.json.gz"],
            ["hcad_lookup_1.json.gz", "hcad_lookup_2.json.gz"],
            ["hcad_lookup.json.gz"],
        ]
        for path_group in local_paths:
            if all(Path(p).exists() for p in path_group):
                try:
                    for p in path_group:
                        log.info("  Loading: %s", p)
                        with gzip.open(p, "rt", encoding="utf-8") as f:
                            raw = json.load(f)
                        for name, v in raw.items():
                            self._lookup[name] = {
                                "mail_address": v.get("a",""),
                                "mail_city":    v.get("c",""),
                                "mail_state":   v.get("s","TX"),
                                "mail_zip":     v.get("z",""),
                                "prop_address": v.get("pa",""),
                                "prop_city":    v.get("pc",""),
                                "prop_state":   "TX",
                                "prop_zip":     v.get("pz",""),
                            }
                    log.info("HCAD lookup ready: %d names", len(self._lookup))
                    self._build_prefix_index()
                    return
                except Exception as e:
                    log.warning("  Failed loading HCAD files: %s", e)

        # Fallback: try downloading from pdata.hcad.org
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        for url in HCAD_URLS:
            try:
                log.info("  Trying download: %s", url)
                r = session.get(url, timeout=300, stream=True)
                if r.status_code == 200:
                    data = r.content
                    log.info("  Downloaded %d MB", len(data)//1024//1024)
                    self._parse_zip(data)
                    if self._lookup:
                        log.info("HCAD lookup ready: %d names", len(self._lookup))
                        self._build_prefix_index()
                        return
            except Exception as e:
                log.warning("  Download failed: %s", e)

        log.warning("HCAD data unavailable — no address enrichment")

    def _parse_zip(self, data: bytes):
        try:
            zf = zipfile.ZipFile(io.BytesIO(data))
        except Exception:
            return
        for name in zf.namelist():
            if "real_acct" in name.lower() and name.endswith(".txt"):
                log.info("  Parsing %s...", name)
                with zf.open(name) as f:
                    reader = csv.DictReader(
                        io.TextIOWrapper(f, encoding="latin-1", errors="replace"),
                        delimiter="\t")
                    for row in reader:
                        owner = row.get("mailto","").strip().upper()
                        if not owner or owner == "CURRENT OWNER":
                            continue
                        self._lookup.setdefault(owner, {
                            "mail_address": row.get("mail_addr_1","").strip(),
                            "mail_city":    row.get("mail_city","").strip(),
                            "mail_state":   (row.get("mail_state","TX") or "TX").strip(),
                            "mail_zip":     row.get("mail_zip","").strip(),
                            "prop_address": row.get("site_addr_1","").strip(),
                            "prop_city":    row.get("site_addr_2","").strip(),
                            "prop_state":   "TX",
                            "prop_zip":     row.get("site_addr_3","").strip(),
                        })
                break

    def _build_prefix_index(self):
        for k in self._lookup:
            if len(k) >= 6:
                self._prefix.setdefault(k[:12], []).append(k)
                self._prefix.setdefault(k[:8], []).append(k)
                self._prefix.setdefault(k[:6], []).append(k)

    def _normalize(self, name: str) -> str:
        name = re.sub(r"[,./]", "", name.upper().strip())
        return re.sub(r"\s+", " ", name)

    def lookup(self, owner: str) -> Optional[dict]:
        if not owner or len(owner.strip()) < 4:
            return None

        # Handle compound names from clerk (e.g. "SMITH JOHN / SMITH MARY")
        # Try each individual name
        if " / " in owner:
            for part in owner.split(" / "):
                result = self._lookup_single(part.strip())
                if result:
                    return result
            return None

        return self._lookup_single(owner)

    def _lookup_single(self, owner: str) -> Optional[dict]:
        if not owner or len(owner.strip()) < 4:
            return None
        name = self._normalize(owner)

        # 1. Exact match
        if name in self._lookup:
            return self._lookup[name]

        # 2. First word + partial match (handles truncation)
        # e.g. "MARTINEZ JOSE" matches "MARTINEZ JOSE LUIS"
        for prefix_len in [14, 12, 10, 8]:
            if len(name) < prefix_len:
                continue
            prefix = name[:prefix_len]
            for candidate in self._prefix.get(prefix, []):
                clen = len(candidate)
                nlen = len(name)
                if (name in candidate or
                    candidate in name or
                    (nlen >= 8 and candidate.startswith(name[:min(nlen, 16)])) or
                    (clen >= 8 and name.startswith(candidate[:min(clen, 16)]))):
                    return self._lookup[candidate]

        # 3. Last name only match for individuals (not LLCs/Corps)
        # e.g. "SMITH JOHN A" → try "SMITH JOHN" prefix
        is_business = any(w in name for w in
            ["LLC","INC","CORP","LTD","TRUST","BANK","NA","LP","LLP",
             "FOUNDATION","ASSOCIATION","CHURCH","SCHOOL","COUNTY",
             "CITY OF","STATE OF","SERVICES","PROPERTIES","HOLDINGS"])

        if not is_business:
            words = name.split()
            if len(words) >= 2:
                # Try lastname + firstname only (ignore middle/suffix)
                short = " ".join(words[:2])
                if len(short) >= 8:
                    for candidate in self._prefix.get(short[:8], []):
                        if candidate.startswith(short):
                            return self._lookup[candidate]

        # 4. For LLCs — try without common suffixes
        if is_business:
            for suffix in [" LLC", " INC", " CORP", " LTD", " LP"]:
                if name.endswith(suffix):
                    trimmed = name[:-len(suffix)].strip()
                    if len(trimmed) >= 6:
                        for candidate in self._prefix.get(trimmed[:8], []):
                            if trimmed in candidate:
                                return self._lookup[candidate]

        return None


# ── SFTP Client ───────────────────────────────────────────────────────────────

class SFTPClient:
    def __init__(self, host, user, password):
        self.host = host; self.user = user; self.password = password
        self._ssh = None; self._sftp = None
        self._home = f"/users/{user}"

    def connect(self):
        log.info("Connecting SFTP: %s", self.host)
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(hostname=self.host, port=22, username=self.user,
                          password=self.password, timeout=30,
                          banner_timeout=30, auth_timeout=30)
        self._sftp = self._ssh.open_sftp()
        try:
            cwd = self._sftp.getcwd()
            if cwd and cwd != "/": self._home = cwd
        except Exception: pass
        log.info("SFTP connected. Home: %s", self._home)

    def disconnect(self):
        try:
            if self._sftp: self._sftp.close()
            if self._ssh:  self._ssh.close()
        except Exception: pass

    def download(self, folder: str, filename: str) -> Optional[bytes]:
        path = f"{self._home}/{folder}/{filename}"
        buf  = io.BytesIO()
        try:
            self._sftp.getfo(path, buf)
            buf.seek(0); data = buf.read()
            log.info("Downloaded %s (%d bytes)", filename, len(data))
            return data
        except FileNotFoundError: return None
        except Exception as e:
            log.warning("SFTP failed %s: %s", path, e); return None

    def get_rp(self, d):   return self.download("Index_RP",   f"{d}_RPISubscriber.zip")
    def get_pro(self, d):  return self.download("Index_PRO",  f"{d}_PROSubscriber.zip")
    def get_frcl(self, d): return self.download("Index_FRCL", f"{d}_FRCLSubscriber.zip")
    def get_asn(self, d):  return self.download("Index_ASN",  f"{d}_ASNSubscriber.zip")


# ── ZIP Parsers ───────────────────────────────────────────────────────────────

def parse_rp_zip(zip_bytes: bytes, date_str: str) -> list:
    records = []
    try: zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad RP zip %s: %s", date_str, e); return []

    znames = {n.lower(): n for n in zf.namelist()}
    instr_key = next((k for k in znames if "instrument" in k), None)
    names_key = next((k for k in znames if "_names" in k), None)
    legal_key = next((k for k in znames if "legal" in k), None)

    if not instr_key:
        log.warning("No instruments in RP zip %s", date_str); return []

    # Parse instruments
    instr_text = zf.read(znames[instr_key]).decode("latin-1", errors="replace")
    instr_rows = {}
    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        fn = (row.get("File No") or row.get("File Number") or "").strip()
        if fn: instr_rows[fn] = row

    # Parse names
    grantor_rows, grantee_rows = {}, {}
    if names_key:
        names_text = zf.read(znames[names_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(names_text), delimiter="|"):
            fn = (row.get("File No") or row.get("File Number") or "").strip()
            if not fn: continue
            ntype = (row.get("NType") or row.get("NameType") or "").strip().upper()
            name  = (row.get("Name") or row.get("FullName") or "").strip()
            addr  = (row.get("Address") or row.get("Addr1") or "").strip()
            city  = (row.get("City") or "").strip()
            state = (row.get("State") or "TX").strip() or "TX"
            zipcd = (row.get("Zip") or row.get("ZipCode") or "").strip()
            entry = {"name": name, "addr": addr, "city": city,
                     "state": state, "zip": zipcd}
            grantor_set = {"G","GR","GRANTOR","1","DR","OR","OBLIGOR"}
            grantee_set = {"E","EE","GRANTEE","2","OE","OBLIGEE","TRUSTEE","BENEFICIARY"}
            if ntype in grantor_set:
                grantor_rows.setdefault(fn, []).append(entry)
            elif ntype in grantee_set:
                grantee_rows.setdefault(fn, []).append(entry)
            elif not grantor_rows.get(fn):
                grantor_rows.setdefault(fn, []).append(entry)
            else:
                grantee_rows.setdefault(fn, []).append(entry)

    # Parse legal
    legal_rows = {}
    if legal_key:
        legal_text = zf.read(znames[legal_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(legal_text), delimiter="|"):
            fn = (row.get("File No") or row.get("File Number") or "").strip()
            if fn:
                desc = (row.get("LegalDescription") or row.get("Legal Description") or
                        row.get("Description") or row.get("Subdivision") or "").strip()
                legal_rows[fn] = desc

    # Build records
    type_counts = {}
    for fn, instr in instr_rows.items():
        raw_type = (instr.get("Document Type") or instr.get("Instrument Type") or "").strip()
        type_counts[raw_type] = type_counts.get(raw_type, 0) + 1

        tmp_grantees = grantee_rows.get(fn, [])
        grantee_name = " / ".join(g["name"] for g in tmp_grantees if g["name"])
        cat, cat_label = classify(raw_type, grantee=grantee_name)
        if not cat: continue

        filed_raw = (instr.get("FileDate") or instr.get("File Date") or date_str).strip()
        filed = norm_date(filed_raw)

        amount = None
        for k in instr.keys():
            if "amount" in k.lower() or "consideration" in k.lower():
                amount = parse_amount(instr[k]); break

        film = (instr.get("Film Code No.") or instr.get("Film Code") or fn).strip()
        clerk_url = (f"https://www.cclerk.hctx.net/Applications/WebSearch/"
                     f"RP_R.aspx?FilmCode={film}") if film else ""

        grantors = grantor_rows.get(fn, [])
        owner_name = " / ".join(g["name"] for g in grantors if g["name"])
        legal = legal_rows.get(fn, "")

        rec = blank_rec(fn, raw_type, cat, cat_label, filed,
                        owner_name, grantee_name, amount, legal, clerk_url)

        # Set mailing address from clerk names file (if available)
        if grantors:
            g = grantors[0]
            rec["mail_address"] = g["addr"]
            rec["mail_city"]    = g["city"]
            rec["mail_state"]   = g["state"]
            rec["mail_zip"]     = g["zip"]

        records.append(rec)

    top = sorted(type_counts.items(), key=lambda x: -x[1])[:8]
    log.info("RP %s: %d records. Types: %s", date_str, len(records), top)
    return records


def parse_pro_zip(zip_bytes: bytes, date_str: str) -> list:
    records = []
    try: zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad PRO zip %s: %s", date_str, e); return []
    znames = {n.lower(): n for n in zf.namelist()}
    instr_key = next((k for k in znames if "instrument" in k), None)
    names_key = next((k for k in znames if "name" in k), None)
    if not instr_key: return []
    instr_text = zf.read(znames[instr_key]).decode("latin-1", errors="replace")
    name_rows = {}
    if names_key:
        nt = zf.read(znames[names_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(nt), delimiter="|"):
            fn = (row.get("File No") or row.get("File Number") or "").strip()
            if fn: name_rows.setdefault(fn, []).append(row)
    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        fn    = (row.get("File No") or row.get("File Number") or "").strip()
        filed = norm_date((row.get("FileDate") or row.get("File Date") or date_str).strip())
        film  = (row.get("Film Code No.") or row.get("Film Code") or fn).strip()
        url   = f"https://www.cclerk.hctx.net/Applications/WebSearch/PRO_R.aspx?FilmCode={film}" if film else ""
        names = name_rows.get(fn, [])
        owner = " / ".join((n.get("Name","") or "").strip() for n in names if (n.get("Name","") or "").strip())
        rec   = blank_rec(fn, "PRO", "PRO", "Probate", filed, owner, clerk_url=url)
        if names:
            n = names[0]
            rec["mail_address"] = (n.get("Address") or "").strip()
            rec["mail_city"]    = (n.get("City") or "").strip()
            rec["mail_state"]   = (n.get("State") or "TX").strip()
            rec["mail_zip"]     = (n.get("Zip") or n.get("ZipCode") or "").strip()
        records.append(rec)
    log.info("PRO %s: %d records", date_str, len(records))
    return records


def parse_frcl_zip(zip_bytes: bytes, date_str: str) -> list:
    records = []
    try: zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad FRCL zip %s: %s", date_str, e); return []
    znames = {n.lower(): n for n in zf.namelist()}
    instr_key = next((k for k in znames if "instrument" in k or "frcl" in k), None)
    if not instr_key: return []
    instr_text = zf.read(znames[instr_key]).decode("latin-1", errors="replace")
    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        fn      = (row.get("File No") or row.get("File Number") or "").strip()
        filed   = norm_date((row.get("FileDate") or row.get("File Date") or
                             row.get("Sale Date") or date_str).strip())
        owner   = (row.get("Grantor Name") or row.get("Name") or row.get("Debtor") or "").strip()
        grantee = (row.get("Grantee Name") or row.get("Trustee") or "").strip()
        amount  = parse_amount(row.get("Amount") or row.get("Consideration") or "")
        legal   = (row.get("Legal Description") or row.get("Description") or "").strip()
        film    = (row.get("Film Code No.") or row.get("Film Code") or fn).strip()
        url     = f"https://www.cclerk.hctx.net/Applications/WebSearch/FRCL_R.aspx?FilmCode={film}" if film else ""
        records.append(blank_rec(fn, "NOFC", "NOFC", "Notice of Foreclosure",
                                 filed, owner, grantee, amount, legal, url))
    log.info("FRCL %s: %d records", date_str, len(records))
    return records


def parse_asn_zip(zip_bytes: bytes, date_str: str) -> list:
    records = []
    try: zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad ASN zip %s: %s", date_str, e); return []
    znames = {n.lower(): n for n in zf.namelist()}
    instr_key = next((k for k in znames if "instrument" in k), None)
    owner_key  = next((k for k in znames if "owner" in k), None)
    dba_key    = next((k for k in znames if "dba" in k), None)
    if not instr_key: return []
    instr_text = zf.read(znames[instr_key]).decode("latin-1", errors="replace")
    owner_rows, dba_rows = {}, {}
    if owner_key:
        ot = zf.read(znames[owner_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(ot), delimiter="|"):
            fn = row.get("File Number","").strip()
            if fn: owner_rows.setdefault(fn, []).append(row)
    if dba_key:
        dt = zf.read(znames[dba_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(dt), delimiter="|"):
            fn = row.get("File Number","").strip()
            if fn: dba_rows[fn] = row
    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        if row.get("Assumed Name or Withdrawn","").strip().upper() == "W": continue
        fn    = row.get("File Number","").strip()
        filed = norm_date(row.get("File Date", date_str))
        film  = row.get("Film Code Number","").strip()
        url   = f"https://www.cclerk.hctx.net/Applications/WebSearch/ASN_R.aspx?FilmCode={film}" if film else ""
        owners   = owner_rows.get(fn, [])
        dba      = dba_rows.get(fn, {})
        dba_name = dba.get("DBA Name","").strip()
        owner_name = " / ".join(o.get("Owner Name","").strip() for o in owners if o.get("Owner Name","").strip())
        rec = blank_rec(fn, "ASN", "ASN", f"Assumed Name: {dba_name}", filed, owner_name, clerk_url=url)
        rec["legal"] = dba_name
        if owners:
            o = owners[0]
            rec["mail_address"] = o.get("Address","").strip()
            rec["mail_city"]    = o.get("City","").strip()
            rec["mail_state"]   = (o.get("State","TX") or "TX").strip()
            rec["mail_zip"]     = o.get("Zip","").strip()
            rec["prop_address"] = dba.get("Address","").strip()
            rec["prop_city"]    = dba.get("City","").strip()
            rec["prop_state"]   = "TX"
            rec["prop_zip"]     = dba.get("Zip","").strip()
        records.append(rec)
    log.info("ASN %s: %d records", date_str, len(records))
    return records


# ── Portal fallback ───────────────────────────────────────────────────────────

def portal_fallback(start: str, end: str) -> list:
    log.info("Portal fallback mode (FTP not configured)")
    records = []
    TYPES = [
        ("LP","LP","Lis Pendens"),("A/J","JUD","Abstract of Judgment"),
        ("CCJ","CCJ","Certified Judgment"),("LNIRS","LNIRS","IRS Lien"),
        ("LNFED","LNFED","Federal Lien"),("LNCORPTX","LNCORPTX","Corp Tax Lien"),
        ("LN","LN","Lien"),("LNMECH","LNMECH","Mechanic Lien"),
        ("LNHOA","LNHOA","HOA Lien"),("TAXDEED","TAXDEED","Tax Deed"),
        ("NOC","NOC","Notice of Commencement"),
    ]
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0"})
    RP  = "https://www.cclerk.hctx.net/Applications/WebSearch/RP.aspx"
    btn = "ctl00$ContentPlaceHolder1$btnSearch"
    for code, cat, label in TYPES:
        try:
            r = session.get(RP, timeout=30); r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            vs = {i.get("name"): i.get("value","")
                  for i in soup.find_all("input", type="hidden") if i.get("name")}
            payload = {**vs,
                "ctl00$ContentPlaceHolder1$txtFrom": start,
                "ctl00$ContentPlaceHolder1$txtTo":   end,
                "ctl00$ContentPlaceHolder1$txtInstrument": code,
                "ctl00$ScriptManager1": f"ctl00$ScriptManager1|{btn}",
                "__ASYNCPOST":"true","__EVENTTARGET":"","__EVENTARGUMENT":"", btn:"Search"}
            r2 = session.post(RP, data=payload, timeout=45, headers={
                "Referer":RP,"Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
                "X-MicrosoftAjax":"Delta=true","X-Requested-With":"XMLHttpRequest",
                "Origin":"https://www.cclerk.hctx.net","Accept":"*/*"})
            m = re.search(r'pageRedirect\|\|([^|]+)', r2.text)
            if not m: continue
            rp = session.get("https://www.cclerk.hctx.net" + unquote(m.group(1)), timeout=30)
            soup2 = BeautifulSoup(rp.text, "lxml")
            for table in soup2.find_all("table"):
                rows = table.find_all("tr")
                if len(rows) < 2: continue
                headers = [td.get_text(" ",strip=True).lower() for td in rows[0].find_all(["th","td"])]
                if not any(k in " ".join(headers) for k in ["file","grantor","names"]): continue
                for row in rows[1:]:
                    cells = row.find_all("td")
                    if not cells: continue
                    data = {headers[i]: cells[i].get_text(" ",strip=True)
                            for i in range(min(len(headers),len(cells)))}
                    link = next((("https://www.cclerk.hctx.net"+a["href"]
                                  if not a["href"].startswith("http") else a["href"])
                                 for cell in cells for a in cell.find_all("a",href=True)), "")
                    def f(*keys):
                        for k in keys:
                            for h in headers:
                                if k in h:
                                    v = data.get(h,"").strip()
                                    if v: return v
                        return ""
                    doc_num = f("file number","number","case number")
                    grantor = f("names","grantor","owner","name")
                    filed   = f("file date","date filed","date")
                    if not doc_num and not grantor: continue
                    records.append(blank_rec(doc_num, label, cat, label,
                                             norm_date(filed) if filed else "", grantor,
                                             clerk_url=link))
            time.sleep(1)
        except Exception as e:
            log.warning("Portal %s failed: %s", code, e)
    return records


# ── GHL CSV Export ────────────────────────────────────────────────────────────

def export_csv(records: list, path: Path):
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
                "First Name":   p[0].title() if p else "",
                "Last Name":    " ".join(p[1:]).title() if len(p)>1 else "",
                "Mailing Address":  r.get("mail_address",""),
                "Mailing City":     r.get("mail_city",""),
                "Mailing State":    r.get("mail_state","TX"),
                "Mailing Zip":      r.get("mail_zip",""),
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
                "Source":           "Harris County Clerk",
                "Public Records URL": r.get("clerk_url",""),
            })
    log.info("GHL CSV → %s", path)


# ── Option B: Scrape addresses from clerk document pages ──────────────────────

def scrape_clerk_address(url: str, session: requests.Session) -> Optional[dict]:
    """
    Visit a Harris County Clerk document detail page and extract
    grantor name, address, and property address from the HTML.

    The RP_R.aspx page shows a table with:
      - Grantor Name + Address
      - Grantee Name + Address
      - Legal Description (often contains property address)
    """
    if not url:
        return None
    try:
        r = session.get(url, timeout=20)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "lxml")

        result = {
            "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
            "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
        }

        # Strategy 1: Find labeled fields in the document detail table
        # Clerk pages use patterns like "Grantor Address:", "Mailing Address:"
        text_blocks = []
        for td in soup.find_all("td"):
            text_blocks.append(td.get_text(" ", strip=True))

        full_text = " ".join(text_blocks)

        # Look for address patterns in the page text
        # Pattern: street number + street name + city + state + zip
        addr_pattern = re.compile(
            r'(\d{1,6}\s+[A-Z][A-Z0-9\s\.\-]+(?:ST|AVE|DR|BLVD|LN|WAY|CT|PL|RD|CIR|TRL|PKWY|HWY|FWY|STE|APT)[\w\s]*)'
            r'[\s,]+([A-Z][A-Z\s]+)'
            r'[\s,]+(TX|TEXAS)'
            r'[\s,]+(\d{5}(?:-\d{4})?)',
            re.IGNORECASE
        )

        # Strategy 2: Parse structured table rows
        # Look for "Grantor" labeled rows which contain name + address
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for i, row in enumerate(rows):
                cells = row.find_all("td")
                cell_text = [c.get_text(" ", strip=True) for c in cells]
                joined = " ".join(cell_text).upper()

                # Grantor row — contains the owner's mailing address
                if any(k in joined for k in ["GRANTOR", "OBLIGOR", "DEBTOR"]):
                    # Next rows often have address components
                    for j in range(i+1, min(i+5, len(rows))):
                        addr_cells = rows[j].find_all("td")
                        addr_text  = " ".join(c.get_text(" ", strip=True) for c in addr_cells)
                        m = addr_pattern.search(addr_text)
                        if m:
                            result["mail_address"] = m.group(1).strip()
                            result["mail_city"]    = m.group(2).strip().title()
                            result["mail_state"]   = "TX"
                            result["mail_zip"]     = m.group(4).strip()
                            break

                # Property / Legal description row
                if any(k in joined for k in ["LEGAL", "PROPERTY", "SITUS", "SITE"]):
                    addr_text = " ".join(cell_text)
                    m = addr_pattern.search(addr_text)
                    if m:
                        result["prop_address"] = m.group(1).strip()
                        result["prop_city"]    = m.group(2).strip().title()
                        result["prop_state"]   = "TX"
                        result["prop_zip"]     = m.group(4).strip()

        # Strategy 3: scan all text for address patterns if nothing found yet
        if not result["mail_address"] and not result["prop_address"]:
            matches = list(addr_pattern.finditer(full_text))
            if matches:
                # First match = likely grantor/mailing address
                m = matches[0]
                result["mail_address"] = m.group(1).strip()
                result["mail_city"]    = m.group(2).strip().title()
                result["mail_state"]   = "TX"
                result["mail_zip"]     = m.group(4).strip()
            if len(matches) > 1:
                # Second match = likely property address
                m = matches[1]
                result["prop_address"] = m.group(1).strip()
                result["prop_city"]    = m.group(2).strip().title()
                result["prop_state"]   = "TX"
                result["prop_zip"]     = m.group(4).strip()

        # Return only if we found something useful
        if result["mail_address"] or result["prop_address"]:
            return result
        return None

    except Exception as e:
        log.debug("Clerk scrape failed %s: %s", url, e)
        return None


def enrich_from_clerk(records: list, max_requests: int = 300) -> int:
    """
    For records still missing addresses after HCAD lookup,
    visit the clerk document page and scrape the address directly.
    Limits to max_requests to stay within GitHub Actions time budget.
    Returns count of newly enriched records.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    missing = [r for r in records
               if not r.get("prop_address") and not r.get("mail_address")
               and r.get("clerk_url")]

    log.info("Clerk address scraping: %d records need addresses (max %d requests)",
             len(missing), max_requests)

    enriched = 0
    for i, r in enumerate(missing[:max_requests]):
        if i > 0 and i % 50 == 0:
            log.info("  Clerk scrape progress: %d/%d enriched so far",
                     enriched, i)

        addr = scrape_clerk_address(r["clerk_url"], session)
        if addr:
            r.update({k: v for k, v in addr.items() if v})
            enriched += 1

        time.sleep(0.5)  # 0.5s delay = ~600 requests/hour, respectful rate limit

    log.info("Clerk scrape complete: %d/%d records enriched",
             enriched, min(len(missing), max_requests))
    return enriched


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== Harris County Scraper v10 ===")
    log.info("FTP: %s | HCAD enrichment: enabled", "YES" if FTP_HOST else "NO (portal fallback)")

    # Build HCAD address lookup
    hcad = HCADLookup()

    # Load HCAD property detail lookup
    prop_lookup = {}
    prop_lookup_path = Path("data/hcad_property_lookup.json.gz")
    if prop_lookup_path.exists():
        import gzip as _gz
        log.info("Loading HCAD property lookup...")
        with _gz.open(prop_lookup_path, "rt", encoding="utf-8") as _f:
            prop_lookup = __import__("json").load(_f)
        log.info("HCAD property lookup ready: %d addresses", len(prop_lookup))
    else:
        log.warning("HCAD property lookup not found - skipping property enrichment")
    hcad.build()

    # Scrape records
    raw = []
    if FTP_HOST and FTP_USER and FTP_PASS and HAS_PARAMIKO:
        sftp = SFTPClient(FTP_HOST, FTP_USER, FTP_PASS)
        try:
            sftp.connect()
            for date_str in business_days_back(LOOKBACK_DAYS):
                log.info("Fetching %s", date_str)
                zb = sftp.get_rp(date_str)
                if zb: raw.extend(parse_rp_zip(zb, date_str))
                zb = sftp.get_pro(date_str)
                if zb: raw.extend(parse_pro_zip(zb, date_str))
                zb = sftp.get_frcl(date_str)
                if zb: raw.extend(parse_frcl_zip(zb, date_str))
                zb = sftp.get_asn(date_str)
                if zb: raw.extend(parse_asn_zip(zb, date_str))
                time.sleep(0.3)
        finally:
            sftp.disconnect()
    else:
        start = cutoff.strftime("%m/%d/%Y")
        end   = now.strftime("%m/%d/%Y")
        raw   = portal_fallback(start, end)

    # Deduplicate
    seen, deduped = set(), []
    for r in raw:
        key = r.get("doc_num") or f"{r.get('owner')}|{r.get('filed')}"
        if key and key not in seen:
            seen.add(key); deduped.append(r)
    log.info("Unique records: %d", len(deduped))

    # Build high-frequency name blocklist (servicers/trustees)
    from collections import Counter as _Counter
    _parts = []
    for _r in deduped:
        for _p in (_r.get("owner") or "").split("/"):
            _parts.append(_p.strip().upper())
    _freq = _Counter(_parts)
    _HIGH_FREQ.update(n for n, c in _freq.items() if c >= 8 and n)

    # Enrich with HCAD addresses + score
    with_addr = 0
    hcad_hits = 0
    for r in deduped:
        try:
            # Try HCAD lookup for property + mailing address
            owner = r.get("owner","")
            if owner and hcad._lookup:
                addr = hcad.lookup(owner)
                if addr:
                    hcad_hits += 1
                    # Only overwrite if we don't already have address from clerk
                    if not r.get("prop_address"):
                        r["prop_address"] = addr["prop_address"]
                        r["prop_city"]    = addr["prop_city"]
                        r["prop_state"]   = addr["prop_state"]
                        r["prop_zip"]     = addr["prop_zip"]
                    if not r.get("mail_address"):
                        r["mail_address"] = addr["mail_address"]
                        r["mail_city"]    = addr["mail_city"]
                        r["mail_state"]   = addr["mail_state"]
                        r["mail_zip"]     = addr["mail_zip"]


            # HCAD property detail lookup (sqft, beds, baths, yr_built)
            prop_addr = r.get("prop_address", "").upper().strip()
            if prop_addr and prop_lookup:
                # Try exact match first
                pdata = prop_lookup.get(prop_addr)
                if not pdata:
                    # Try without city/state suffix
                    parts = prop_addr.split(",")
                    if parts:
                        pdata = prop_lookup.get(parts[0].strip())
                if pdata:
                    r["sqft"]       = pdata.get("sqft", 0)
                    r["yr_built"]   = pdata.get("yr_built", "")
                    r["beds"]       = pdata.get("beds", 0)
                    r["full_baths"] = pdata.get("full_baths", 0)
                    r["half_baths"] = pdata.get("half_baths", 0)
                    r["hcad_acct"]  = pdata.get("acct", "")
            if r.get("prop_address") or r.get("mail_address"):
                with_addr += 1

            # Extract primary owner
            owner_raw = r.get("owner", "")
            r["primary_owner"] = _extract_primary(owner_raw, _HIGH_FREQ)
            r["owner_is_person"] = _is_person(r["primary_owner"])

            r["score"], r["flags"] = compute_score(r, cutoff)
        except Exception as e:
            log.debug("Enrich error: %s", e)
            r.setdefault("score", 10); r.setdefault("flags", [])

    log.info("HCAD matches: %d/%d (%.0f%%)", hcad_hits, len(deduped),
             100*hcad_hits/len(deduped) if deduped else 0)

    # Log remaining unmatched
    still_missing = sum(1 for r in deduped if not r.get("prop_address") and not r.get("mail_address"))
    log.info("Still missing addresses: %d (likely new LLCs or out-of-county owners)", still_missing)

    deduped.sort(key=lambda x: x.get("score", 0), reverse=True)

    payload = {
        "fetched_at":   now.isoformat(),
        "source":       "Harris County Clerk FTP + HCAD",
        "date_range":   {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total":        len(deduped),
        "with_address": with_addr,
        "records":      deduped,
    }

    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        (d/"records.json").write_text(json.dumps(payload, indent=2, default=str))
        log.info("Saved → %s/records.json", d)

    # Write lightweight stats snapshot for dashboards (no CORS needed)
    warm_count = sum(1 for r in deduped if 40 <= r.get("score", 0) < 70)
    hot_count  = sum(1 for r in deduped if r.get("score", 0) >= 70)
    stats = {
        "fetched_at":   now.isoformat(),
        "total":        len(deduped),
        "with_address": with_addr,
        "warm":         warm_count,
        "hot":          hot_count,
        "date_range":   {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
    }
    for d in OUTPUT_DIRS:
        stats_path = d / "leadiq_stats.json"
        stats_path.write_text(json.dumps(stats, indent=2, default=str))
        log.info("Stats snapshot -> %s", stats_path)

    export_csv(deduped, Path("data/leads_ghl.csv"))
    log.info("Done. total=%d with_address=%d", len(deduped), with_addr)


if __name__ == "__main__":
    main()
