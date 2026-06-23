import re
with open('scraper/multi_county_pubsearch.py', encoding='utf-8') as f:
    content = f.read()
old = '''DOC_TYPES = [
    "Lis Pendens", "Tax Deed", "Abstract of Judgment",
    "Federal Tax Lien", "State Tax Lien", "Mechanic Lien",
    "Probate", "Divorce", "Judgment", "Notice of Foreclosure",
]'''
new = '''DOC_TYPES = [
    "Lis Pendens", "Tax Deed", "Abstract of Judgment",
    "Federal Tax Lien", "Mechanic Lien", "Probate",
]'''
if old in content:
    content = content.replace(old, new)
    with open('scraper/multi_county_pubsearch.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: 10 -> 6 doc types")
else:
    print("FAIL - showing current:")
    idx = content.find("DOC_TYPES")
    print(repr(content[idx:idx+200]))
