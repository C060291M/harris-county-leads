with open('.github/workflows/scrape_portals.yml', encoding='utf-8') as f:
    content = f.read()

new_jobs = """
  lubbock:
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
      - run: python -m playwright install --with-deps chromium
      - name: Run Lubbock scraper
        run: python scraper/multi_county_lubbock.py
        env:
          LOOKBACK_DAYS: "3"
          MAX_PAGES: "5"
      - run: python scraper/push_to_db.py
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}

  tomgreen:
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
      - run: python -m playwright install --with-deps chromium
      - name: Run Tom Green scraper
        run: python scraper/multi_county_tomgreen.py
        env:
          LOOKBACK_DAYS: "3"
          MAX_PAGES: "5"
      - run: python scraper/push_to_db.py
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
"""

content = content.rstrip() + "\n" + new_jobs
with open('.github/workflows/scrape_portals.yml', 'w', encoding='utf-8') as f:
    f.write(content)
print("Added Lubbock and Tom Green to portals workflow")
