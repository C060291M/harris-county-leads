with open('scraper/tyler_scraper_universal.py', encoding='utf-8') as f:
    content = f.read()

old = '    "Eastland":    ("https://eastlandcountytx-web.tylerhost.net/web",          "DOCSEARCH144S1"),'
new = '''    "Eastland":    ("https://eastlandcountytx-web.tylerhost.net/web",          "DOCSEARCH144S1"),
    "Erath":       ("https://erathcountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),'''

if old in content:
    content = content.replace(old, new)
    with open('scraper/tyler_scraper_universal.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Added Erath")
else:
    print("FAIL - anchor not found")
