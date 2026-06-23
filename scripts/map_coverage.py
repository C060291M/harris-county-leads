# Counties in workflows vs counties in DB
import re, os

# Get all counties referenced in workflows
wf_counties = set()
wf_dir = '.github/workflows'
for wf in os.listdir(wf_dir):
    if not wf.endswith('.yml'): continue
    with open(os.path.join(wf_dir, wf), encoding='utf-8') as f:
        content = f.read()
    # Find COUNTY: and COUNTIES: values
    for m in re.findall(r'COUNTY[S]?:\s*"([^"]+)"', content):
        for c in m.split(','):
            wf_counties.add(c.strip().lower())

# Get all counties in tyler registry
with open('scraper/tyler_scraper_universal.py', encoding='utf-8') as f:
    tyler = f.read()
tyler_counties = set(re.findall(r'"([A-Z][^"]+)":\s*\("https://.*?tylerhost', tyler))

# Get all counties in pubsearch
with open('scraper/multi_county_pubsearch.py', encoding='utf-8') as f:
    pub = f.read()
pub_counties = set(re.findall(r'"([A-Z][^"]+)":\s*\("https://.*?publicsearch', pub))

# Get i2i counties
with open('scraper/multi_county_i2i.py', encoding='utf-8') as f:
    i2i = f.read()
i2i_counties = set(re.findall(r'"([A-Z][^"]+)":\s*"https://i2i', i2i))

all_built = tyler_counties | pub_counties | i2i_counties
all_built_lower = {c.lower() for c in all_built}

print(f"Tyler registry:    {len(tyler_counties)} counties")
print(f"PublicSearch:      {len(pub_counties)} counties")
print(f"i2i:               {len(i2i_counties)} counties")
print(f"Total built:       {len(all_built)} unique")
print(f"In workflows:      {len(wf_counties)}")
print(f"\nBUILT but NOT in any workflow:")
missing_from_wf = all_built_lower - wf_counties
for c in sorted(missing_from_wf):
    print(f"  {c}")

print(f"\nIn WORKFLOW but NOT in any scraper:")
missing_from_scraper = wf_counties - all_built_lower - {'harris', 'travis'}
for c in sorted(missing_from_scraper):
    print(f"  {c}")
