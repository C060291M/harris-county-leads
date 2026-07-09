import re, json, os, logging, httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("travis_adv")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
COUNTY = "travis"
BASE = "https://www.tccsearch.org"
SEARCH_URL = f"{BASE}/RealEstate/SearchEntry.aspx"
HEADERS = {"User-Agent":"Mozilla/5.0"}

DISTRESS_TYPES = {63:"LP", 1:"AJ", 51:"FEDTAX", 65:"ML", 25:"PROB", 107:"STTAX", 72:"FORECLOSURE", 24:"JUDGMT"}
CAT_MAP = {63:("LP","Lis Pendens"),1:("JUD","Abstract of Judgment"),51:("LNFED","Federal Tax Lien"),
           65:("LNMECH","Mechanic Lien"),25:("PRO","Probate"),107:("LNSTATE","State Tax Lien"),72:("NOFC","Foreclosure"),24:("JUD","Judgment")}

def make_cs(dt):
    s = "01%d-%d-%d-0-0-0-0" % (dt.year, dt.month, dt.day)
    return "|0|" + s + "||[[[[]],[],[]]," + '[{},[]],"' + s + '"]'

def get_vs(soup):
    out = {}
    for fld in ["__VIEWSTATE","__EVENTVALIDATION","__VIEWSTATEGENERATOR"]:
        el = soup.find("input", {"name": fld}); out[fld] = el.get("value","") if el else ""
    return out

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def parse_rows(html, seen):
    soup = BeautifulSoup(html, "lxml")
    recs = []
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 5: continue
        texts = [c.get_text(" ", strip=True) for c in cells]
        doc_num = next((t.strip() for t in texts if re.match(r"^\d{10}$", t.strip())), None)
        if not doc_num or doc_num in seen: continue
        seen.add(doc_num)
        filed = next((norm_date(t.strip()) for t in texts if re.match(r"\d{2}/\d{2}/\d{4}", t.strip())), "")
        if filed and filed < "2025-01-01": continue
        name_col = " ".join(texts)
        rm = re.search(r'\[R\]\s*([^\[]+)', name_col)
        em = re.search(r'\[E\]\s*([^\[]+)', name_col)
        owner = re.sub(r'\s+', ' ', rm.group(1)).strip() if rm else ""
        if em:
            grantee_raw = em.group(1)
            # Travis echoes grantor+grantee again after a standalone "R" marker
            # (hidden sort/index columns) - cut it off there.
            grantee_raw = re.split(r'\s+R\s+[A-Z]{2,}', grantee_raw)[0]
            grantee = re.sub(r'\s+', ' ', grantee_raw).strip()
        else:
            grantee = ""
        if not owner or len(owner) < 3: continue
        recs.append({"doc_num":doc_num,"doc_type":"","cat":"LN","cat_label":"Lien",
            "filed":filed,"owner":owner,"grantee":grantee,"amount":None,"legal":"",
            "county":COUNTY,"clerk_url":f"{BASE}/RealEstate/SearchResults.aspx",
            "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"","score":15,"flags":[]})
    return recs

def do_search(client, start_dt, end_dt, doc_idx, seen):
    r = client.get(SEARCH_URL, timeout=15)
    soup = BeautifulSoup(r.text,"lxml")
    vs = get_vs(soup)
    def gc(n): el=soup.find("input",{"name":n}); return el.get("value","") if el else ""
    
    form = {**vs,
        "__EVENTTARGET":"ctl00$cphNoMargin$SearchButtons2$btnSearch","__EVENTARGUMENT":"0",
        "Header1_WebHDS_clientState":"","Header1_WebDataMenu1_clientState":gc("Header1_WebDataMenu1_clientState"),
        "ctl00$cphNoMargin$f$NameSearchMode":"rdoCombine",
        "cphNoMargin_f_txtParty_clientState":gc("cphNoMargin_f_txtParty_clientState"),
        "cphNoMargin_f_txtParty":"","ctl00$cphNoMargin$f$drbPartyType":"",
        "cphNoMargin_f_txtGrantor_clientState":gc("cphNoMargin_f_txtGrantor_clientState"),
        "cphNoMargin_f_txtGrantee_clientState":gc("cphNoMargin_f_txtGrantee_clientState"),
        "cphNoMargin_f_ddcDateFiledFrom_clientState":make_cs(start_dt),
        "cphNoMargin_f_ddcDateFiledTo_clientState":make_cs(end_dt),
        "cphNoMargin_f_txtInstrumentNoFrom_clientState":gc("cphNoMargin_f_txtInstrumentNoFrom_clientState"),
        "cphNoMargin_f_txtInstrumentNoFrom":"","cphNoMargin_f_txtInstrumentNoTo_clientState":gc("cphNoMargin_f_txtInstrumentNoTo_clientState"),
        "cphNoMargin_f_txtInstrumentNoTo":"","cphNoMargin_f_txtBook_clientState":gc("cphNoMargin_f_txtBook_clientState"),
        "cphNoMargin_f_txtBook":"","cphNoMargin_f_txtPage_clientState":gc("cphNoMargin_f_txtPage_clientState"),
        "cphNoMargin_f_txtPage":"","cphNoMargin_f_DataTextEdit1_clientState":gc("cphNoMargin_f_DataTextEdit1_clientState"),
        "cphNoMargin_f_DataTextEdit1":"","cphNoMargin_f_txtLDStreetAddress_clientState":gc("cphNoMargin_f_txtLDStreetAddress_clientState"),
        "cphNoMargin_f_txtLDStreetAddress":"","cphNoMargin_f_txtLDLot_clientState":gc("cphNoMargin_f_txtLDLot_clientState"),
        "cphNoMargin_f_txtLDLot":"","cphNoMargin_f_txtLDBook_clientState":gc("cphNoMargin_f_txtLDBook_clientState"),
        "cphNoMargin_f_txtLDBook":"","cphNoMargin_f_txtLDSection_clientState":gc("cphNoMargin_f_txtLDSection_clientState"),
        "cphNoMargin_f_txtLDSection":"","cphNoMargin_f_txtLDVolume_clientState":gc("cphNoMargin_f_txtLDVolume_clientState"),
        "cphNoMargin_f_txtLDVolume":"","cphNoMargin_f_txtLDFreeForm_clientState":gc("cphNoMargin_f_txtLDFreeForm_clientState"),
        "cphNoMargin_f_txtLDFreeForm":"","cphNoMargin_dlgPopup_clientState":gc("cphNoMargin_dlgPopup_clientState"),
        "dlgOptionWindow_clientState":gc("dlgOptionWindow_clientState"),"RangeContextMenu_clientState":gc("RangeContextMenu_clientState"),
        "LoginForm1_txtLogonName_clientState":gc("LoginForm1_txtLogonName_clientState"),"LoginForm1_txtLogonName":"",
        "LoginForm1_txtPassword_clientState":gc("LoginForm1_txtPassword_clientState"),"LoginForm1_txtPassword":"",
        "ctl00$LoginForm1$logonType":"rdoPubCpu","_ig_def_dp_cal_clientState":gc("_ig_def_dp_cal_clientState"),
        "ctl00$cphNoMargin$SearchButtons2$btnSearch__10":":0",
        f"ctl00$cphNoMargin$f$dclDocType${doc_idx}":DISTRESS_TYPES[doc_idx],
    }
    r = client.post(SEARCH_URL, data=form, headers={**HEADERS,"Content-Type":"application/x-www-form-urlencoded"}, timeout=15)
    recs = parse_rows(r.text, seen)
    text = BeautifulSoup(r.text,"lxml").get_text(" ",strip=True)
    total_m = re.search(r"([\d,]+)\s+records?\s+found", text, re.I)
    total = int(total_m.group(1).replace(",","")) if total_m else 0
    return recs, total

def scrape():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    all_records = []
    seen = set()
    
    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=20) as client:
        r = client.get(BASE)
        soup = BeautifulSoup(r.text,"lxml")
        vs = get_vs(soup)
        client.post(BASE, data={**vs,"__EVENTTARGET":"ctl00$cph1$lnkAccept","__EVENTARGUMENT":""},
                   headers={**HEADERS,"Content-Type":"application/x-www-form-urlencoded"})
        
        for doc_idx, val in DISTRESS_TYPES.items():
            cat, lbl = CAT_MAP[doc_idx]
            count = 0
            # Split into 3-day windows to stay under 20-record cap
            current = cutoff
            while current < now:
                end = min(current + timedelta(days=3), now)
                try:
                    recs, total = do_search(client, current, end, doc_idx, seen)
                    for rec in recs:
                        rec["cat"] = cat; rec["cat_label"] = lbl
                        rec["score"] = {"JUD":35,"LNFED":45,"PRO":25,"LP":20,"NOFC":20,"LNSTATE":20,"LN":15}.get(cat,10)
                    all_records.extend(recs)
                    count += len(recs)
                    if total > 0:
                        log.info(f"  [{doc_idx}] {current.strftime('%m/%d')}-{end.strftime('%m/%d')}: {len(recs)} new (total={total})")
                except Exception as e:
                    log.warning(f"  [{doc_idx}] {current.strftime('%m/%d')}: {e}")
                current = end + timedelta(days=1)
            
            log.info(f"Travis [{doc_idx}] {lbl}: {count} records")
    
    log.info(f"Travis total: {len(all_records)} unique records")
    os.makedirs("dashboard", exist_ok=True)
    with open("dashboard/travis_adv_records.json","w") as f:
        json.dump({"county":COUNTY,"total":len(all_records),"records":all_records},f)
    log.info("Saved -> dashboard/travis_adv_records.json")

scrape()
