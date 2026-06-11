import json

with open('county_registry.json') as f:
    registry = json.load(f)

# Group by platform
from collections import defaultdict
by_platform = defaultdict(list)
for r in registry:
    by_platform[r['platform']].append(r['county'])

print('=== LIVE PORTALS BY PLATFORM ===')
for platform, counties in sorted(by_platform.items(), key=lambda x: -len(x[1])):
    print(f'\n{platform.upper()} ({len(counties)} counties):')
    for c in sorted(counties):
        print(f'  {c}')

# Find counties already in our scrapers
import os, re
scraper_dir = 'scraper'
scraped_counties = set()
for f in os.listdir(scraper_dir):
    if not f.startswith('multi_county_'): continue
    with open(f'{scraper_dir}/{f}') as fh:
        content = fh.read()
    matches = re.findall(r'"([A-Z][a-zA-Z ]+)"\s*[:\(]', content)
    for m in matches:
        scraped_counties.add(m.lower().replace(' ',''))

registry_counties = set(r['county'].replace(' ','') for r in registry)
not_scraped = registry_counties - scraped_counties
print(f'\n=== NOT YET SCRAPED ({len(not_scraped)} counties) ===')
for c in sorted(not_scraped):
    matches = [r for r in registry if r['county'].replace(' ','') == c]
    for m in matches:
        print(f'  {c:<20} {m["platform"]:<15} {m["url"]}')
