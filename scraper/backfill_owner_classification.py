import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import re
import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DB = os.environ["DATABASE_URL"]

PLACEHOLDER_VALUES = {
    "public", "the public", "ex parte", "see instrument", "unknown",
    "n/a", "na", "none", "the",
}

INSTITUTIONAL_PATTERNS = [
    r"\bbank\b", r"\bn\.?\s?a\.?\b", r"\bmortgage\b", r"\bcredit\b",
    r"\bllc\b", r"\binc\b", r"\bl\s?l\s?c\b", r"\bl\s?p\b", r"\bcorp\b",
    r"\bcorporation\b", r"\bcompany\b", r"\bco\.?\b", r"\bassociation\b",
    r"\bhoa\b", r"\bhomeowners\b", r"\bcommunity\b", r"\bimprovement\b",
    r"\bfunding\b", r"\bfinance\b", r"\bfinancial\b", r"\binvestments?\b",
    r"\bcapital\b", r"\bproperties\b", r"\bhomes?\s+of\s+texas\b",
    r"\bhome\s?builders?\b", r"\bservicing\b", r"\bservices\b",
    r"\btrustee\b", r"\bnominee\b", r"\bmers\b", r"\bfannie\s?mae\b",
    r"\bfreddie\s?mac\b", r"\bhud\b", r"\bhousing\s+and\s+urban\b",
    r"\bhousing\b", r"\bcommission\b", r"\bdepartment\b", r"\bagency\b",
    r"\bauthority\b", r"\binsurance\b", r"\bwater\s+supply\b",
    r"\bcounty\b", r"\bcity\s+of\b", r"\bstate\s+of\s?texa?s?\b",
    r"\bi\s?s\s?d\b", r"\bm\s?u\s?d\b", r"\bschool\s+district\b",
    r"\bunited\s+states?\b", r"\bu\.?\s?s\.?\s?a?\.?\b(?!\w)",
    r"\bworkforce\b", r"\brailroad\b", r"\btexas\s+department\b",
    r"\btax\s+solutions?\b", r"\brecovery\b", r"\bmanagement\b",
    r"\bstaffing\b", r"\bpowersports?\b", r"\bpipeline\b", r"\bresorts?\b",
    r"\bgroup\b", r"\bfund\b", r"\bseries\b", r"\bsociety\b",
    r"\blaw\s+office\b",
    r"\bestates\b", r"\bacres\b", r"\baddition\b", r"\bsubdivision\b",
    r"\bcrossing\b", r"\bmeadows\b", r"\bplaza\b", r"\bvillas?\b",
    r"\blanding\b", r"\btrails?\b", r"\bboating\b",
    r"\bcollege\b", r"\bhospital\b", r"\bministry\b", r"\bconstruction\b",
    r"\bconst\b", r"\bsupply\b", r"\bdistrict\b", r"\bemergency\s+serv",
    r"\bclinic\b", r"\bfoundation\b", r"\bbuilders?\b", r"\brealty\b",
    r"\btitle\s+company\b", r"\bengineering\b", r"\bcontractors?\b",
]

INSTITUTIONAL_RE = re.compile("|".join(INSTITUTIONAL_PATTERNS), re.IGNORECASE)
CHURCH_RE = re.compile(r"\bchurch\b", re.IGNORECASE)


def clean_name(name):
    if not name:
        return name
    name = re.sub(r"\s+(Temp\s+)?[A-Z]?\s?OPR?\d{5,}.*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s+\d{6,}.*$", "", name)
    return name.strip()


def split_parties(field):
    if not field:
        return []
    return [p.strip() for p in field.split("/") if p.strip()]


def is_placeholder(name):
    return name.strip().lower() in PLACEHOLDER_VALUES


def _has_smashed_bank(word):
    w = word.lower()
    return w.endswith("bank") and w != "bank" and not w.endswith("banks")


def _has_institutional_church(name):
    m = CHURCH_RE.search(name)
    if not m:
        return False
    first_word = name.strip().split()[0].lower() if name.strip() else ""
    return first_word != "church"


def is_institutional(name):
    if is_placeholder(name):
        return None
    if INSTITUTIONAL_RE.search(name):
        return True
    if _has_institutional_church(name):
        return True
    for word in re.findall(r"[A-Za-z]+", name):
        if _has_smashed_bank(word):
            return True
    return False


def is_suspicious_single_word(name):
    words = name.split()
    return len(words) == 1 and len(words[0]) > 4


def classify_field(field):
    field = clean_name(field)
    parts = split_parties(field)
    if not parts:
        return (False, [], False, True, [])

    real_parts = [p for p in parts if not is_placeholder(p)]
    if not real_parts:
        return (False, [], False, True, [])

    person_parts = []
    institutional_parts = []
    suspicious_parts = []
    for p in real_parts:
        if is_institutional(p):
            institutional_parts.append(p)
        elif is_suspicious_single_word(p):
            suspicious_parts.append(p)
        else:
            person_parts.append(p)

    has_person = len(person_parts) > 0
    is_pure_institutional = len(person_parts) == 0 and len(institutional_parts) > 0
    return (has_person, person_parts, is_pure_institutional, False, suspicious_parts)


def classify_pair(owner, grantee):
    o_has_person, o_persons, o_pure_inst, _, o_susp = classify_field(owner)
    g_has_person, g_persons, g_pure_inst, _, g_susp = classify_field(grantee)

    if o_has_person and g_has_person:
        return {"homeowner": " / ".join(o_persons), "co_owner": " / ".join(g_persons), "lienholder": None, "confidence": "medium"}
    if o_has_person and not g_has_person:
        return {"homeowner": " / ".join(o_persons), "co_owner": None, "lienholder": grantee if g_pure_inst else None, "confidence": "high"}
    if g_has_person and not o_has_person:
        return {"homeowner": " / ".join(g_persons), "co_owner": None, "lienholder": owner if o_pure_inst else None, "confidence": "high"}
    if o_pure_inst and g_pure_inst:
        return {"homeowner": None, "co_owner": None, "lienholder": f"{owner} -> {grantee}", "confidence": "high"}
    return {"homeowner": None, "co_owner": None, "lienholder": None, "confidence": "low"}


def get_conn():
    return psycopg2.connect(DB, connect_timeout=30)


def main():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, owner, grantee FROM lead_records WHERE grantee IS NOT NULL")
    rows = cur.fetchall()
    logger.info(f"Found {len(rows)} records to (re)classify")

    updated = 0
    for lead_id, owner, grantee in rows:
        try:
            result = classify_pair(owner or "", grantee or "")
            cur.execute("""
                UPDATE lead_records SET
                    homeowner_name = %s,
                    co_owner_name = %s,
                    lienholder_name = %s,
                    classification_confidence = %s
                WHERE id = %s
            """, (
                result["homeowner"], result["co_owner"],
                result["lienholder"], result["confidence"], lead_id
            ))
            updated += 1
            if updated % 200 == 0:
                conn.commit()
                logger.info(f"{updated}/{len(rows)} classified so far (committed)")
        except Exception as e:
            logger.warning(f"Lead {lead_id}: {e}")
            continue

    conn.commit()
    logger.info(f"Done: {updated}/{len(rows)} records classified")

    cur.execute("SELECT classification_confidence, COUNT(*) FROM lead_records WHERE classification_confidence IS NOT NULL GROUP BY classification_confidence")
    logger.info("Confidence breakdown:")
    for row in cur.fetchall():
        logger.info(f"  {row[0]}: {row[1]}")

    cur.execute("SELECT COUNT(*) FROM lead_records WHERE homeowner_name IS NOT NULL")
    logger.info(f"Total leads with a real homeowner name identified: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM lead_records WHERE co_owner_name IS NOT NULL")
    logger.info(f"Total leads with a co-owner captured: {cur.fetchone()[0]}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
