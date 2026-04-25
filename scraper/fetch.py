"""
Harris County Clerk SFTP Scraper v9 - Production
SFTP: sftp.cclerk.hctx.net
Folder: /users/<user>/Index_RP/
Files:  YYYYMMDD_RPISubscriber.zip
Confirmed column names from live server:
  RPI_Instruments.txt: File No | Document Type | FileDate | Film Code No. | No. of Pages
  RPI_Names.txt:       File No | Name | NType | Document Type
  RPI_LegalDesc.txt:   File No | Legal Description (approx)
"""
import csv, io, json, logging, os, re, sys, time, zipfile
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

OUTPUT_DIRS   = [Path("dashboard"), Path("data")]
DEBUG_DIR     = Path("debug")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

FTP_HOST = os.environ.get("FTP_HOST", "")
FTP_USER = os.environ.get("FTP_USER", "")
FTP_PASS = os.environ.get("FTP_PASS", "")

RP_CAT_MAP = {
    "LP":           ("LP",       "Lis Pendens"),
    "RELLP":        ("RELLP",    "Release Lis Pendens"),
    "A/J":          ("JUD",      "Abstract of Judgment"),
    "ABST OF JUD":  ("JUD",      "Abstract of Judgment"),
    "ABSTRACT OF JUDGMENT": ("JUD", "Abstract of Judgment"),
    "CCJ":          ("CCJ",      "Certified Judgment"),
    "CERT JUDG":    ("CCJ",      "Certified Judgment"),
    "DRJUD":        ("DRJUD",    "Domestic Relations Judgment"),
    "LNIRS":        ("LNIRS",    "IRS Lien"),
    "IRS LIEN":     ("LNIRS",    "IRS Lien"),
    "LNFED":        ("LNFED",    "Federal Lien"),
    "FED LIEN":     ("LNFED",    "Federal Lien"),
    "LNCORPTX":     ("LNCORPTX", "Corp Tax Lien"),
    "CORP TAX":     ("LNCORPTX", "Corp Tax Lien"),
    "LN":           ("LN",       "Lien"),
    "LIEN":         ("LN",       "Lien"),
    "LNMECH":       ("LNMECH",   "Mechanic Lien"),
    "MECH LIEN":    ("LNMECH",   "Mechanic Lien"),
    "MECHANIC":     ("LNMECH",   "Mechanic Lien"),
    "LNHOA":        ("LNHOA",    "HOA Lien"),
    "HOA LIEN":     ("LNHOA",    "HOA Lien"),
    "MEDLN":        ("MEDLN",    "Medicaid Lien"),
    "MEDICAID":     ("MEDLN",    "Medicaid Lien"),
    "TAXDEED":      ("TAXDEED",  "Tax Deed"),
    "TAX DEED":     ("TAXDEED",  "Tax Deed"),
    "NOC":          ("NOC",      "Notice of Commencement"),
    "NOTICE OF COM":("NOC",      "Notice of Commencement"),
    "NOFC":         ("NOFC",     "Notice of Foreclosure"),
    "FORECLOSURE":  ("NOFC",     "Notice of Foreclosure"),
    "PRO":          ("PRO",      "Probate"),
    "PROBATE":      ("PRO",      "Probate"),
}


def parse_amount(text) -> Optional[float]:
    if not text:
        return None
    c = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        v = float(c)
        return v if v > 0 else None
    except ValueError:
        return None


def norm_date(raw: str) -> str:
    raw = raw.strip()
    if re.match(r"^\d{8}$", raw):
        try:
            return datetime.strptime(raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def name_variants(name: str):
    name = name.strip().upper()
    v = {name}
    if "," in name:
        p = [x.strip() for x in name.split(",", 1)]
        if len(p) == 2:
            v.add(f"{p[1]} {p[0]}")
    else:
        p = name.split()
        if len(p) >= 2:
            v.add(f"{p[-1]}, {' '.join(p[:-1])}")
            v.add(f"{p[-1]} {' '.join(p[:-1])}")
    return v


def business_days_back(n: int) -> list:
    dates = []
    d = datetime.now()
    while len(dates) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            dates.append(d.strftime("%Y%m%d"))
    return dates


def classify(raw_type: str):
    t = raw_type.strip().upper()
    if t in RP_CAT_MAP:
        return RP_CAT_MAP[t]
    for key, val in RP_CAT_MAP.items():
        if key and t and (key in t or t in key):
            return val
    return None, None


def compute_score(record: dict, cutoff: datetime):
    flags, s = [], 10
    cat   = record.get("cat", "")
    amt   = record.get("amount")
    owner = (record.get("owner") or "").upper()
    filed = record.get("filed", "")
    legal = (record.get("legal") or "").upper()
    dtype = (record.get("doc_type") or "").upper()

    if cat in ("TAXDEED", "LNIRS", "LNCORPTX", "LNFED"):
        flags.append("Tax delinquency"); s += 30
    if cat in ("LNMECH", "LNHOA"):
        flags.append("Code / HOA violation"); s += 25
    if cat == "PRO":
        flags.append("Probate / estate"); s += 20
    if cat in {"LN", "LNMECH", "LNHOA", "LNIRS", "LNFED", "LNCORPTX", "MEDLN"}:
        if "Lien on record" not in flags:
            flags.append("Lien on record"); s += 15
    if any(k in legal + dtype for k in ["DIVORCE", "DISSOLUTION", "BANKRUPTCY"]) or cat == "DRJUD":
        flags.append("Divorce / bankruptcy"); s += 10
    if cat in ("LP", "RELLP"):
        flags.append("Lis pendens"); s += 10
    if cat == "NOFC":
        flags.append("Pre-foreclosure"); s += 10
    if cat in ("JUD", "CCJ"):
        flags.append("Judgment lien"); s += 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        s += 20
    if re.search(r"\b(LLC|CORP|INC|LTD|TRUST)\b", owner):
        flags.append("LLC / corp owner")
    try:
        if datetime.strptime(filed[:10], "%Y-%m-%d") >= cutoff:
            flags.append("New this week"); s += 5
    except Exception:
        pass
    if amt:
        if amt > 100000: s += 15
        elif amt > 50000: s += 10
    if record.get("prop_address") or record.get("mail_address"):
        s += 5
    s += len(flags) * 2
    return min(s, 100), flags


def blank_rec(fn, dtype, cat, cat_label, filed, owner,
              grantee="", amount=None, legal="", url=""):
    return {
        "doc_num": fn, "doc_type": dtype,
        "cat": cat, "cat_label": cat_label,
        "filed": filed, "owner": owner, "grantee": grantee,
        "amount": amount, "legal": legal, "clerk_url": url,
        "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
    }


class SFTPClient:
    def __init__(self, host, user, password):
        self.host     = host
        self.user     = user
        self.password = password
        self._ssh     = None
        self._sftp    = None
        self._home    = f"/users/{user}"

    def connect(self):
        log.info("Connecting SFTP: %s", self.host)
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(
            hostname=self.host, port=22,
            username=self.user, password=self.password,
            timeout=30, banner_timeout=30, auth_timeout=30,
        )
        self._sftp = self._ssh.open_sftp()
        try:
            cwd = self._sftp.getcwd()
            if cwd and cwd != "/":
                self._home = cwd
        except Exception:
            pass
        log.info("SFTP connected. Home: %s", self._home)

    def disconnect(self):
        try:
            if self._sftp: self._sftp.close()
            if self._ssh:  self._ssh.close()
        except Exception:
            pass

    def download(self, folder: str, filename: str) -> Optional[bytes]:
        path = f"{self._home}/{folder}/{filename}"
        buf  = io.BytesIO()
        try:
            self._sftp.getfo(path, buf)
            buf.seek(0)
            data = buf.read()
            log.info("Downloaded %s (%d bytes)", filename, len(data))
            return data
        except FileNotFoundError:
            return None
        except Exception as e:
            log.warning("SFTP download failed %s: %s", path, e)
            return None

    def get_rp(self, date_str: str) -> Optional[bytes]:
        return self.download("Index_RP", f"{date_str}_RPISubscriber.zip")

    def get_pro(self, date_str: str) -> Optional[bytes]:
        return self.download("Index_PRO", f"{date_str}_PROSubscriber.zip")

    def get_frcl(self, date_str: str) -> Optional[bytes]:
        return self.download("Index_FRCL", f"{date_str}_FRCLSubscriber.zip")

    def get_asn(self, date_str: str) -> Optional[bytes]:
        return self.download("Index_ASN", f"{date_str}_ASNSubscriber.zip")


def parse_rp_zip(zip_bytes: bytes, date_str: str) -> list:
    """
    Parse Real Property zip.
    Confirmed column names from live server (2026-04-23 logs):
      RPI_Instruments.txt: File No | Document Type | FileDate | Film Code No. | No. of Pages
      RPI_Names.txt:       File No | Name | NType | Document Type
    """
    records = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad RP zip %s: %s", date_str, e)
        return []

    znames = {n.lower(): n for n in zf.namelist()}

    instr_key = next((k for k in znames if "instrument" in k), None)
    names_key = next((k for k in znames if "_names" in k), None)
    legal_key = next((k for k in znames if "legal" in k), None)

    if not instr_key:
        log.warning("No instruments file in RP zip %s", date_str)
        return []

    # ── Instruments ──────────────────────────────────────────────────────────
    instr_text = zf.read(znames[instr_key]).decode("latin-1", errors="replace")
    instr_rows = {}
    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        fn = (row.get("File No") or row.get("File Number") or
              row.get("FileNo") or "").strip()
        if fn:
            instr_rows[fn] = row

    log.info("RP %s: %d instrument rows", date_str, len(instr_rows))

    # ── Names (grantors + grantees) ───────────────────────────────────────────
    grantor_rows = {}
    grantee_rows = {}
    if names_key:
        names_text = zf.read(znames[names_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(names_text), delimiter="|"):
            fn = (row.get("File No") or row.get("File Number") or
                  row.get("FileNo") or "").strip()
            if not fn:
                continue
            ntype = (row.get("NType") or row.get("NameType") or
                     row.get("Name Type") or row.get("Party") or "").strip().upper()
            name  = (row.get("Name") or row.get("FullName") or "").strip()
            addr  = (row.get("Address") or row.get("Addr1") or row.get("Addr") or "").strip()
            city  = (row.get("City") or "").strip()
            state = (row.get("State") or "TX").strip() or "TX"
            zipcd = (row.get("Zip") or row.get("ZipCode") or row.get("ZIP") or "").strip()
            entry = {"name": name, "addr": addr, "city": city, "state": state, "zip": zipcd}
            # NType: G/GR/GRANTOR = grantor; E/EE/GRANTEE = grantee
            if ntype in ("G", "GR", "GRANTOR", "1", "DR", "OR"):
                grantor_rows.setdefault(fn, []).append(entry)
            elif ntype in ("E", "EE", "GRANTEE", "2", "EE", "OE"):
                grantee_rows.setdefault(fn, []).append(entry)
            else:
                # Default first party = grantor
                grantor_rows.setdefault(fn, []).append(entry)

    # ── Legal descriptions ────────────────────────────────────────────────────
    legal_rows = {}
    if legal_key:
        legal_text = zf.read(znames[legal_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(legal_text), delimiter="|"):
            fn = (row.get("File No") or row.get("File Number") or
                  row.get("FileNo") or "").strip()
            if fn:
                desc = (row.get("LegalDescription") or row.get("Legal Description") or
                        row.get("Description") or row.get("Subdivision") or
                        " ".join(str(v) for v in list(row.values())[1:3])).strip()
                legal_rows[fn] = desc

    # ── Build records ─────────────────────────────────────────────────────────
    type_counts = {}
    for fn, instr in instr_rows.items():
        # Exact column names confirmed: "Document Type", "FileDate", "Film Code No."
        raw_type = (instr.get("Document Type") or instr.get("Instrument Type") or
                    instr.get("Doc Type") or "").strip()
        type_counts[raw_type] = type_counts.get(raw_type, 0) + 1

        cat, cat_label = classify(raw_type)
        if not cat:
            continue

        filed_raw = (instr.get("FileDate") or instr.get("File Date") or
                     instr.get("FileDate") or date_str).strip()
        filed = norm_date(filed_raw)

        amount = None
        for k in instr.keys():
            if "amount" in k.lower() or "consideration" in k.lower():
                amount = parse_amount(instr[k])
                break

        film = (instr.get("Film Code No.") or instr.get("Film Code") or
                instr.get("FilmCode") or fn).strip()
        clerk_url = (f"https://www.cclerk.hctx.net/Applications/WebSearch/"
                     f"RP_R.aspx?FilmCode={film}") if film else ""

        grantors = grantor_rows.get(fn, [])
        grantees = grantee_rows.get(fn, [])
        legal    = legal_rows.get(fn, "")

        owner_name   = " / ".join(g["name"] for g in grantors if g["name"])
        grantee_name = " / ".join(g["name"] for g in grantees if g["name"])

        rec = blank_rec(fn, raw_type, cat, cat_label, filed,
                        owner_name, grantee_name, amount, legal, clerk_url)

        if grantors:
            g = grantors[0]
            rec["mail_address"] = g["addr"]
            rec["mail_city"]    = g["city"]
            rec["mail_state"]   = g["state"]
            rec["mail_zip"]     = g["zip"]

        records.append(rec)

    top_types = sorted(type_counts.items(), key=lambda x: -x[1])[:8]
    log.info("RP %s: %d target records. Top doc types: %s",
             date_str, len(records), top_types)
    return records


def parse_pro_zip(zip_bytes: bytes, date_str: str) -> list:
    records = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad PRO zip %s: %s", date_str, e)
        return []

    znames = {n.lower(): n for n in zf.namelist()}
    instr_key = next((k for k in znames if "instrument" in k), None)
    names_key = next((k for k in znames if "name" in k), None)
    if not instr_key:
        return []

    instr_text = zf.read(znames[instr_key]).decode("latin-1", errors="replace")
    name_rows  = {}
    if names_key:
        nt = zf.read(znames[names_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(nt), delimiter="|"):
            fn = (row.get("File No") or row.get("File Number") or "").strip()
            if fn:
                name_rows.setdefault(fn, []).append(row)

    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        fn    = (row.get("File No") or row.get("File Number") or "").strip()
        filed = norm_date((row.get("FileDate") or row.get("File Date") or date_str).strip())
        film  = (row.get("Film Code No.") or row.get("Film Code") or fn).strip()
        clerk_url = (f"https://www.cclerk.hctx.net/Applications/WebSearch/"
                     f"PRO_R.aspx?FilmCode={film}") if film else ""
        names  = name_rows.get(fn, [])
        owner  = " / ".join((n.get("Name","") or "").strip() for n in names if n.get("Name","").strip())
        rec    = blank_rec(fn, "PRO", "PRO", "Probate", filed, owner, clerk_url=clerk_url)
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
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad FRCL zip %s: %s", date_str, e)
        return []

    znames = {n.lower(): n for n in zf.namelist()}
    instr_key = next((k for k in znames if "instrument" in k or "frcl" in k), None)
    if not instr_key:
        return []

    instr_text = zf.read(znames[instr_key]).decode("latin-1", errors="replace")
    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        fn      = (row.get("File No") or row.get("File Number") or "").strip()
        filed   = norm_date((row.get("FileDate") or row.get("File Date") or
                             row.get("Sale Date") or date_str).strip())
        owner   = (row.get("Grantor Name") or row.get("Name") or
                   row.get("Debtor") or "").strip()
        grantee = (row.get("Grantee Name") or row.get("Trustee") or "").strip()
        amount  = parse_amount(row.get("Amount") or row.get("Consideration") or "")
        legal   = (row.get("Legal Description") or row.get("Description") or "").strip()
        film    = (row.get("Film Code No.") or row.get("Film Code") or fn).strip()
        clerk_url = (f"https://www.cclerk.hctx.net/Applications/WebSearch/"
                     f"FRCL_R.aspx?FilmCode={film}") if film else ""
        rec = blank_rec(fn, "NOFC", "NOFC", "Notice of Foreclosure",
                        filed, owner, grantee, amount, legal, clerk_url)
        records.append(rec)

    log.info("FRCL %s: %d records", date_str, len(records))
    return records


def parse_asn_zip(zip_bytes: bytes, date_str: str) -> list:
    records = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except Exception as e:
        log.warning("Bad ASN zip %s: %s", date_str, e)
        return []

    znames = {n.lower(): n for n in zf.namelist()}
    instr_key = next((k for k in znames if "instrument" in k), None)
    owner_key = next((k for k in znames if "owner" in k), None)
    dba_key   = next((k for k in znames if "dba" in k), None)
    if not instr_key:
        return []

    instr_text = zf.read(znames[instr_key]).decode("latin-1", errors="replace")
    owner_rows = {}
    dba_rows   = {}

    if owner_key:
        ot = zf.read(znames[owner_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(ot), delimiter="|"):
            fn = row.get("File Number","").strip()
            if fn:
                owner_rows.setdefault(fn, []).append(row)

    if dba_key:
        dt = zf.read(znames[dba_key]).decode("latin-1", errors="replace")
        for row in csv.DictReader(io.StringIO(dt), delimiter="|"):
            fn = row.get("File Number","").strip()
            if fn:
                dba_rows[fn] = row

    for row in csv.DictReader(io.StringIO(instr_text), delimiter="|"):
        if row.get("Assumed Name or Withdrawn","").strip().upper() == "W":
            continue
        fn    = row.get("File Number","").strip()
        filed = norm_date(row.get("File Date", date_str))
        film  = row.get("Film Code Number","").strip()
        clerk_url = (f"https://www.cclerk.hctx.net/Applications/WebSearch/"
                     f"ASN_R.aspx?FilmCode={film}") if film else ""
        owners   = owner_rows.get(fn, [])
        dba      = dba_rows.get(fn, {})
        dba_name = dba.get("DBA Name","").strip()
        owner_name = " / ".join(o.get("Owner Name","").strip()
                                for o in owners if o.get("Owner Name","").strip())
        rec = blank_rec(fn, "ASN", "ASN", f"Assumed Name: {dba_name}",
                        filed, owner_name, clerk_url=clerk_url)
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

    log.info("ASN %s: %d active records", date_str, len(records))
    return records


def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


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
    session = make_session()
    RP = "https://www.cclerk.hctx.net/Applications/WebSearch/RP.aspx"
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
                "__ASYNCPOST": "true", "__EVENTTARGET": "", "__EVENTARGUMENT": "",
                btn: "Search"}
            r2 = session.post(RP, data=payload, timeout=45, headers={
                "Referer": RP,
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-MicrosoftAjax": "Delta=true",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "https://www.cclerk.hctx.net", "Accept": "*/*"})
            m = re.search(r'pageRedirect\|\|([^|]+)', r2.text)
            if not m:
                continue
            redir = "https://www.cclerk.hctx.net" + unquote(m.group(1))
            rp = session.get(redir, timeout=30)
            soup2 = BeautifulSoup(rp.text, "lxml")
            for table in soup2.find_all("table"):
                rows = table.find_all("tr")
                if len(rows) < 2: continue
                headers = [td.get_text(" ",strip=True).lower()
                           for td in rows[0].find_all(["th","td"])]
                if not any(k in " ".join(headers) for k in ["file","grantor","names"]): continue
                for row in rows[1:]:
                    cells = row.find_all("td")
                    if not cells: continue
                    data = {headers[i]: cells[i].get_text(" ",strip=True)
                            for i in range(min(len(headers),len(cells)))}
                    link = next((("https://www.cclerk.hctx.net"+a["href"]
                                  if not a["href"].startswith("http") else a["href"])
                                 for cell in cells
                                 for a in cell.find_all("a", href=True)), "")
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


def export_csv(records: list, path: Path):
    cols = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State",
            "Mailing Zip","Property Address","Property City","Property State","Property Zip",
            "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
            "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in records:
            p   = (r.get("owner") or "").strip().split()
            amt = r.get("amount")
            w.writerow({
                "First Name":   p[0].title() if p else "",
                "Last Name":    " ".join(p[1:]).title() if len(p) > 1 else "",
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


def main():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    log.info("=== Harris County Scraper v9 ===")
    log.info("FTP configured: %s", bool(FTP_HOST and FTP_USER and FTP_PASS))

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

    with_addr = 0
    for r in deduped:
        try:
            r["score"], r["flags"] = compute_score(r, cutoff)
            if r.get("prop_address") or r.get("mail_address"):
                with_addr += 1
        except Exception as e:
            log.debug("Score error: %s", e)
            r.setdefault("score", 10)
            r.setdefault("flags", [])

    deduped.sort(key=lambda x: x.get("score", 0), reverse=True)

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
        (d / "records.json").write_text(json.dumps(payload, indent=2, default=str))
        log.info("Saved → %s/records.json", d)

    export_csv(deduped, Path("data/leads_ghl.csv"))
    log.info("Done. total=%d with_address=%d", len(deduped), with_addr)


if __name__ == "__main__":
    main()
