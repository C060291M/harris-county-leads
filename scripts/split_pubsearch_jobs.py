with open('.github/workflows/scrape_pubsearch.yml', encoding='utf-8') as f:
    content = f.read()

# Replace multi-county COUNTIES values with single counties
# Split each multi-county group into separate jobs
replacements = {
    'COUNTIES: "Hidalgo,El Paso"':      'COUNTIES: "Hidalgo"',
    'COUNTIES: "Midland,Grayson"':      'COUNTIES: "Midland"',
    'COUNTIES: "Montgomery,Bell"':      'COUNTIES: "Montgomery"',
    'COUNTIES: "Wilson,Milam,Bee"':     'COUNTIES: "Wilson"',
    'COUNTIES: "Chambers,Walker,Madison"': 'COUNTIES: "Chambers"',
    'COUNTIES: "Zapata,Medina,Coleman"':   'COUNTIES: "Zapata"',
    'COUNTIES: "Rusk,Panola,Brewster"':    'COUNTIES: "Rusk"',
    'COUNTIES: "Victoria,Calhoun,Bosque,Coryell"': 'COUNTIES: "Victoria"',
    'COUNTIES: "Anderson,Nacogdoches,Grimes"': 'COUNTIES: "Anderson"',
    'COUNTIES: "Guadalupe,Kendall,Matagorda"': 'COUNTIES: "Guadalupe"',
    'COUNTIES: "Jim Wells,Starr,San Patricio,Freestone"': 'COUNTIES: "Jim Wells"',
    'COUNTIES: "Reeves,Potter,Burleson"': 'COUNTIES: "Reeves"',
    'COUNTIES: "Hockley,Refugio,Goliad,Red River"': 'COUNTIES: "Hockley"',
}

for old, new in replacements.items():
    if old in content:
        content = content.replace(old, new)
        print(f"Fixed: {old} -> {new}")

with open('.github/workflows/scrape_pubsearch.yml', 'w', encoding='utf-8') as f:
    f.write(content)
print("Done - all multi-county jobs now run single county")
