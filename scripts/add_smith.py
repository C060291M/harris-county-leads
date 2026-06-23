with open('scraper/multi_county_pubsearch.py', encoding='utf-8') as f:
    content = f.read()

old = '    "Tarrant":    ("https://tarrant.tx.publicsearch.us",    "RP"),'
new = '    "Tarrant":    ("https://tarrant.tx.publicsearch.us",    "RP"),\n    "Smith":      ("https://smith.tx.publicsearch.us",       "RP"),'

if old in content:
    content = content.replace(old, new)
    print('Smith added to pubsearch')

with open('scraper/multi_county_pubsearch.py', 'w', encoding='utf-8') as f:
    f.write(content)
