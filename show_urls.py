import os, re
results = {}
for fname in os.listdir("scraper"):
    if not fname.startswith("multi_county_") or not fname.endswith(".py"): continue
    with open(f"scraper/{fname}", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    m = re.search(r'BASE_URL\s*=\s*"([^"]+)"', content)
    if m:
        county = fname.replace("multi_county_","").replace(".py","")
        results[county] = m.group(1)

# Show counties with non-standard URLs
for county, url in sorted(results.items()):
    if "tylerhost" in url:
        print(f"{county}: {url}")
