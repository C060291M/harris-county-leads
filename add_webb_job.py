content = open(".github/workflows/scrape.yml", encoding="utf-8").read()

webb_job = """
  webb:
    runs-on: ubuntu-22.04
    timeout-minutes: 120
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
          cache-dependency-path: scraper/requirements.txt
      - run: pip install -r scraper/requirements.txt
      - run: python -m playwright install --with-deps chromium
      - name: Run Webb County scraper (Kofile GovOS)
        run: python scraper/multi_county_webb.py
        env:
          LOOKBACK_DAYS: ${{ github.event.inputs.lookback_days || '3' }}
          MAX_PAGES: ${{ github.event.inputs.max_pages || '8' }}
      - name: Push to DB
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
        run: python3 scraper/push_to_db.py
"""

content = content.replace("  enrich-esearch:", webb_job + "  enrich-esearch:")
content = content.replace(
    "needs: [harris, dallas-bexar, tarrant, collin, travis, fort-bend, mclennan,",
    "needs: [harris, dallas-bexar, tarrant, collin, travis, fort-bend, mclennan, webb,"
)

open(".github/workflows/scrape.yml", "w", encoding="utf-8").write(content)
print("done")
