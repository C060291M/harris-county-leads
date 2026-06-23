import re

with open('scraper/multi_county_pubsearch.py', encoding='utf-8') as f:
    content = f.read()

old = '    "Tarrant":     ("https://tarrant.tx.publicsearch.us",     "RP"),'
new = '''    "Tarrant":     ("https://tarrant.tx.publicsearch.us",     "RP"),
    "Denton":      ("https://denton.tx.publicsearch.us",      "RP"),
    "Collin":      ("https://collin.tx.publicsearch.us",      "RP"),
    "Johnson":     ("https://johnson.tx.publicsearch.us",     "RP"),'''

if old in content:
    content = content.replace(old, new, 1)
    with open('scraper/multi_county_pubsearch.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS")
else:
    print("FAIL - anchor not found")
    print(repr(content[content.find('"Tarrant"'):content.find('"Tarrant"')+80]))
