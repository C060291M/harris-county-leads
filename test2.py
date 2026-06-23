import re

sample = """2026029543
A OF J
6/23/2026 1:41:23 PM
UNOFFICIAL
2026029543
6/23/2026 1:41:23 PM
A OF J"""

KEEP_DOC_TYPES = {"A OF J", "ABSTRACT", "JUDGMENT"}

lines = [l.strip() for l in sample.split("\n") if l.strip()]
i = 0
while i < len(lines):
    if re.match(r"^20\d{8}$", lines[i]):
        doc_num = lines[i]
        j = i + 1
        doc_type = ""
        date_raw = ""
        while j < len(lines) and j < i + 8:
            l = lines[j]
            if l == doc_num or l == "UNOFFICIAL": j += 1; continue
            if re.match(r"^\d{1,2}/\d{1,2}/\d{4}", l): date_raw = l; j += 1; continue
            if l in ("Page Count:", "Parties", "Legals", "Additional", "Party 1:", "Party 2:"): break
            if not doc_type: doc_type = l
            j += 1
        match = any(k in doc_type.upper() for k in KEEP_DOC_TYPES)
        print(f"{doc_num} | type='{doc_type}' | date='{date_raw}' | match={match}")
        i = j
    else:
        i += 1
