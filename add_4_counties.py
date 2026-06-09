content = open("scraper/multi_county_15.py", encoding="utf-8").read()

old = '''    "Bosque": "bosque.tx.publicsearch.us",
    "Coryell": "coryell.tx.publicsearch.us",
    "Falls": "falls.tx.publicsearch.us",
    "Robertson": "robertson.tx.publicsearch.us",
    "Limestone": "limestone.tx.publicsearch.us",'''

new = '''    "Bosque":       "bosque.tx.publicsearch.us",
    "Coryell":      "coryell.tx.publicsearch.us",
    "Falls":        "falls.tx.publicsearch.us",
    "Robertson":    "robertson.tx.publicsearch.us",
    "Limestone":    "limestone.tx.publicsearch.us",
    "Jim Hogg":     "jimhogg.tx.publicsearch.us",
    "Jim Wells":    "jimwells.tx.publicsearch.us",
    "Red River":    "redriver.tx.publicsearch.us",
    "San Patricio": "sanpatricio.tx.publicsearch.us",'''

content = content.replace(old, new)
open("scraper/multi_county_15.py", "w", encoding="utf-8").write(content)
print("done")
