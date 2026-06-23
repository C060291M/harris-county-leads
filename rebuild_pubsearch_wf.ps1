# Each pubsearch county gets its own parallel job
# Groups of ~5 counties per job to stay under 90min

$counties_groups = @(
    @("Tarrant"),
    @("Dallas"),
    @("Bexar"),
    @("Smith"),
    @("Hidalgo", "El Paso"),
    @("Brazos"),
    @("Midland", "Grayson"),
    @("Montgomery", "Bell"),
    @("Wilson", "Milam", "Bee"),
    @("Chambers", "Walker", "Madison"),
    @("Zapata", "Medina", "Coleman"),
    @("Rusk", "Panola", "Brewster"),
    @("Victoria", "Calhoun", "Bosque", "Coryell"),
    @("Anderson", "Nacogdoches", "Grimes"),
    @("Guadalupe", "Kendall", "Matagorda"),
    @("Jim Wells", "Starr", "San Patricio", "Freestone"),
    @("Reeves", "Potter", "Burleson"),
    @("Hockley", "Refugio", "Goliad", "Red River"),
    @("Jim Hogg")
)

$jobs = ""
$idx = 1
foreach ($group in $counties_groups) {
    $jobname = "pubsearch-$idx"
    $counties_str = $group -join ","
    $skip_str = (@("Tarrant","Dallas","Bexar","Smith","Hidalgo","El Paso","Brazos","Midland","Grayson","Montgomery","Bell","Wilson","Milam","Bee","Chambers","Walker","Madison","Zapata","Medina","Coleman","Rusk","Panola","Brewster","Victoria","Calhoun","Bosque","Coryell","Anderson","Nacogdoches","Grimes","Guadalupe","Kendall","Matagorda","Jim Wells","Starr","San Patricio","Freestone","Reeves","Potter","Burleson","Hockley","Refugio","Goliad","Red River","Jim Hogg") | Where-Object { $group -notcontains $_ }) -join ","
    
    $jobs += @"

  $jobname`:
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
          COUNTIES: "$counties_str"
      - run: python scraper/push_to_db.py
        env:
          DATABASE_URL: `${{ secrets.DATABASE_URL }}
"@
    $idx++
}

$yml = @"
name: StackIQ PublicSearch Scraper

on:
  schedule:
    - cron: "30 8 * * *"
  workflow_dispatch: {}

jobs:
$jobs
"@

[System.IO.File]::WriteAllText(".github\workflows\scrape_pubsearch.yml", $yml, [System.Text.Encoding]::UTF8)
Write-Host "scrape_pubsearch.yml written with $($idx-1) parallel jobs"
