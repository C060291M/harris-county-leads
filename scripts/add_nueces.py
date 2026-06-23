with open('scraper/multi_county_pubsearch.py', encoding='utf-8') as f:
    content = f.read()

old = '    "Johnson":     ("https://johnson.tx.publicsearch.us",     "RP"),'
new = '''    "Johnson":     ("https://johnson.tx.publicsearch.us",     "RP"),
    "Nueces":      ("https://nueces.tx.publicsearch.us",      "RP"),'''

if old in content:
    content = content.replace(old, new, 1)
    with open('scraper/multi_county_pubsearch.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: Nueces added")
else:
    print("FAIL")
