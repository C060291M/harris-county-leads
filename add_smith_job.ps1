$wf = ".github\workflows\scrape_pubsearch.yml"
$content = Get-Content $wf -Raw -Encoding UTF8

$smithJob = @"

  smith:
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
          COUNTIES: "Smith"
      - run: python scraper/push_to_db.py
        env:
          DATABASE_URL: `${{ secrets.DATABASE_URL }}
          JSON_GLOB: "dashboard/pubsearch_records.json"
"@

# Also fix other-pubsearch to skip Smith too
$content = $content -replace 'SKIP_COUNTIES: "Tarrant,Dallas"', 'SKIP_COUNTIES: "Tarrant,Dallas,Smith"'

# Append smith job before end of file
$content = $content.TrimEnd() + "`n" + $smithJob + "`n"

[System.IO.File]::WriteAllText($wf, $content, [System.Text.Encoding]::UTF8)
Write-Host "Smith job added to scrape_pubsearch.yml"
Write-Host "Verifying..."
Select-String -Path $wf -Pattern "smith:"
