# Counties that got dropped when we split multi-county jobs
# Each needs its own job in the workflow
dropped = [
    "El Paso", "Grayson", "Bell", "Milam", "Bee",
    "Walker", "Madison", "Medina", "Coleman",
    "Panola", "Brewster", "Calhoun", "Bosque", "Coryell",
    "Nacogdoches", "Grimes", "Kendall", "Matagorda",
    "Starr", "San Patricio", "Freestone",
    "Potter", "Burleson", "Refugio", "Goliad", "Red River",
]

job_template = """
  {jobname}:
    runs-on: ubuntu-22.04
    timeout-minutes: 90
    continue-on-error: true
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
          cache-dependency-path: scraper/requirements.txt
      - run: pip install -r scraper/requirements.txt
      - run: playwright install chromium --with-deps
      - run: python scraper/multi_county_pubsearch.py
        env:
          LOOKBACK_DAYS: "3"
          COUNTIES: "{county}"
      - run: python scraper/push_to_db.py
        env:
          DATABASE_URL: ${{{{ secrets.DATABASE_URL }}}}"""

with open('.github/workflows/scrape_pubsearch.yml', encoding='utf-8') as f:
    content = f.read()

new_jobs = ""
for county in dropped:
    jobname = county.lower().replace(" ", "-").replace(".", "")
    new_jobs += job_template.format(jobname=jobname, county=county)

content = content.rstrip() + "\n" + new_jobs
with open('.github/workflows/scrape_pubsearch.yml', 'w', encoding='utf-8') as f:
    f.write(content)
print(f"Added {len(dropped)} county jobs")
