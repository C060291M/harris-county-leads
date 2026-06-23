with open('scraper/multi_county_pubsearch.py', encoding='utf-8') as f:
    content = f.read()

# Find the COUNTIES dict and replace it with the full list
old = '''    "Tarrant":    ("https://tarrant.tx.publicsearch.us",    "RP"),
    "Smith":      ("https://smith.tx.publicsearch.us",       "RP"),
    "Montgomery": ("https://montgomery.tx.publicsearch.us", "RP"),
    "Bell":       ("https://bell.tx.publicsearch.us",       "RP"),
    "Medina":     ("https://medina.tx.publicsearch.us",     "RP"),
    "Coleman":    ("https://coleman.tx.publicsearch.us",    "RP"),
    "Brewster":   ("https://brewster.tx.publicsearch.us",   "RP"),
    "Hockley":    ("https://hockley.tx.publicsearch.us",    "RP"),
    "Refugio":    ("https://refugio.tx.publicsearch.us",    "RP"),'''

new = '''    "Tarrant":     ("https://tarrant.tx.publicsearch.us",     "RP"),
    "Dallas":      ("https://dallas.tx.publicsearch.us",      "RP"),
    "Bexar":       ("https://bexar.tx.publicsearch.us",       "RP"),
    "Smith":       ("https://smith.tx.publicsearch.us",       "RP"),
    "Montgomery":  ("https://montgomery.tx.publicsearch.us",  "RP"),
    "Hidalgo":     ("https://hidalgo.tx.publicsearch.us",     "RP"),
    "El Paso":     ("https://elpaso.tx.publicsearch.us",      "RP"),
    "Brazos":      ("https://brazos.tx.publicsearch.us",      "RP"),
    "Bee":         ("https://bee.tx.publicsearch.us",         "RP"),
    "Midland":     ("https://midland.tx.publicsearch.us",     "RP"),
    "Wilson":      ("https://wilson.tx.publicsearch.us",      "RP"),
    "Milam":       ("https://milam.tx.publicsearch.us",       "RP"),
    "Chambers":    ("https://chambers.tx.publicsearch.us",    "RP"),
    "Walker":      ("https://walker.tx.publicsearch.us",      "RP"),
    "Madison":     ("https://madison.tx.publicsearch.us",     "RP"),
    "Zapata":      ("https://zapata.tx.publicsearch.us",      "RP"),
    "Medina":      ("https://medina.tx.publicsearch.us",      "RP"),
    "Grayson":     ("https://grayson.tx.publicsearch.us",     "RP"),
    "Bell":        ("https://bell.tx.publicsearch.us",        "RP"),
    "Rusk":        ("https://rusk.tx.publicsearch.us",        "RP"),
    "Panola":      ("https://panola.tx.publicsearch.us",      "RP"),
    "Brewster":    ("https://brewster.tx.publicsearch.us",    "RP"),
    "Coleman":     ("https://coleman.tx.publicsearch.us",     "RP"),
    "Victoria":    ("https://victoria.tx.publicsearch.us",    "RP"),
    "Calhoun":     ("https://calhoun.tx.publicsearch.us",     "RP"),
    "Bosque":      ("https://bosque.tx.publicsearch.us",      "RP"),
    "Coryell":     ("https://coryell.tx.publicsearch.us",     "RP"),
    "Hockley":     ("https://hockley.tx.publicsearch.us",     "RP"),
    "Refugio":     ("https://refugio.tx.publicsearch.us",     "RP"),
    "Anderson":    ("https://anderson.tx.publicsearch.us",    "RP"),
    "Nacogdoches": ("https://nacogdoches.tx.publicsearch.us", "RP"),
    "Grimes":      ("https://grimes.tx.publicsearch.us",      "RP"),
    "Guadalupe":   ("https://guadalupe.tx.publicsearch.us",   "RP"),
    "Kendall":     ("https://kendall.tx.publicsearch.us",     "RP"),
    "Matagorda":   ("https://matagorda.tx.publicsearch.us",   "RP"),
    "Jim Wells":   ("https://jimwells.tx.publicsearch.us",    "RP"),
    "Starr":       ("https://starr.tx.publicsearch.us",       "RP"),
    "San Patricio":("https://sanpatricio.tx.publicsearch.us", "RP"),
    "Freestone":   ("https://freestone.tx.publicsearch.us",   "RP"),
    "Reeves":      ("https://reeves.tx.publicsearch.us",      "RP"),
    "Potter":      ("https://potter.tx.publicsearch.us",      "RP"),
    "Burleson":    ("https://burleson.tx.publicsearch.us",    "RP"),
    "Jim Hogg":    ("https://jimhogg.tx.publicsearch.us",     "RP"),
    "Goliad":      ("https://goliad.tx.publicsearch.us",      "RP"),
    "Red River":   ("https://redriver.tx.publicsearch.us",    "RP"),'''

if old in content:
    content = content.replace(old, new)
    with open('scraper/multi_county_pubsearch.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: added", new.count('"RP"'), "counties to multi_county_pubsearch.py")
else:
    print("FAIL: county dict not found exactly")
    # Show what we have
    import re
    m = re.search(r'"Tarrant".*?"Refugio"[^\n]*', content, re.DOTALL)
    if m: print("Found block:", m.group(0)[:300])
