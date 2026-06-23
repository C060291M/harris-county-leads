with open('.github/workflows/scrape_pubsearch.yml', encoding='utf-8') as f:
    content = f.read()

new_jobs = """
  denton:
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
          COUNTIES: "Denton"
      - run: python scraper/push_to_db.py
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}

  collin:
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
          COUNTIES: "Collin"
      - run: python scraper/push_to_db.py
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}

  johnson:
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
          COUNTIES: "Johnson"
      - run: python scraper/push_to_db.py
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
"""

content = content.rstrip() + "\n" + new_jobs
with open('.github/workflows/scrape_pubsearch.yml', 'w', encoding='utf-8') as f:
    f.write(content)
print("Added Denton, Collin, Johnson jobs")
