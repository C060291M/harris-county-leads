import os, glob

# Extract DOCSEARCH IDs and BASE_URLs from all county scrapers
import re

counties = {}
for fpath in sorted(glob.glob("scraper/multi_county_*.py")):
    with open(fpath, encoding="utf-8", errors="ignore") as f:
        content = f.read()
    
    # Only process Tyler iDS scrapers (have DOCSEARCH in them)
    if "DOCSEARCH" not in content:
        continue
    if "tylerhost.net" not in content and "tx-web.tyler" not in content:
        continue
    
    # Extract county name
    county_m = re.search(r'"county":\s*"([^"]+)"', content)
    if not county_m:
        county_m = re.search(r"log\s*=\s*logging\.getLogger\(['\"](\w+)['\"]", content)
    county = county_m.group(1) if county_m else ""
    
    # Extract BASE_URL
    base_m = re.search(r'BASE_URL\s*=\s*["\']([^"\']+)["\']', content)
    base_url = base_m.group(1) if base_m else ""
    
    # Extract DOCSEARCH ID
    doc_m = re.search(r'DOCSEARCH(\w+)', content)
    docsearch = "DOCSEARCH" + doc_m.group(1) if doc_m else ""
    
    if county and base_url and docsearch:
        counties[county] = {"base_url": base_url, "docsearch": docsearch}

print(f"Found {len(counties)} Tyler counties:")
for k, v in sorted(counties.items()):
    print(f"  {k:<20} {v['docsearch']:<20} {v['base_url']}")
