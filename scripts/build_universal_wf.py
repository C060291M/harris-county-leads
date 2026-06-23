wf_jobs = []

# All counties that need the universal scraper
COUNTIES = [
    ("Henderson",  "https://hendersoncountytx-web.tylerhost.net/web"),
    ("Hood",       "https://hoodcountytx-web.tylerhost.net/web"),
    ("Lamar",      "https://lamarcountytx-web.tylerhost.net/web"),
    ("Lavaca",     "https://lavacacountytx-web.tylerhost.net/web"),
    ("Liberty",    "https://libertycountytx-web.tylerhost.net/web"),
    ("Wood",       "https://woodcountytx-web.tylerhost.net/web"),
    ("VanZandt",   "https://vanzandtcountytx-web.tylerhost.net/web"),
    ("Kaufman",    "https://kaufmancountytx-web.tylerhost.net/web"),
    ("Gregg",      "https://greggcountytx-web.tylerhost.net/web"),
    ("Harrison",   "https://harrisoncountytx-web.tylerhost.net/web"),
    ("Upshur",     "https://upshurcountytx-web.tylerhost.net/web"),
    ("Andrews",    "https://andrewscountytx-web.tylerhost.net/web"),
    ("Wise",       "https://wisecountytx-web.tylerhost.net/web"),
    ("Polk",       "https://polkcountytx-web.tylerhost.net/web"),
    ("Somervell",  "https://somervellcountytx-web.tylerhost.net/web"),
    ("Randall",    "https://randallcountytx-web.tylerhost.net/web"),
    ("Waller",     "https://wallercountytx-web.tylerhost.net/web"),
    ("Orange",     "https://orangecountytx-web.tylerhost.net/web"),
    ("Hardin",     "https://hardincountytx-web.tylerhost.net/web"),
    ("Jasper",     "https://jaspercountytx-web.tylerhost.net/web"),
    ("Panola",     "https://panolacountytx-web.tylerhost.net/web"),
    ("Karnes",     "https://karnescountytx-web.tylerhost.net/web"),
    ("Howard",     "https://howardcountytx-web.tylerhost.net/web"),
    ("Wichita",    "https://wichitacountytx-web.tylerhost.net/web"),
    ("Bastrop",    "https://bastroptx-web.tylerhost.net/web"),
    ("Hays",       "https://hayscountytx-web.tylerhost.net/web"),
    ("Comal",      "https://comalcountytx-web.tylerhost.net/web"),
    ("Guadalupe",  "https://guadalupecountytx-web.tylerhost.net/web"),
    ("Hamilton",   "https://hamiltoncountytx-web.tylerhost.net/web"),
    ("Eastland",   "https://eastlandcountytx-web.tylerhost.net/web"),
    ("Erath",      "https://erathcountytx-web.tylerhost.net/web"),
    ("Gonzales",   "https://gonzalescountytx-web.tylerhost.net/web"),
    ("Calhoun",    "https://calhouncountytx-web.tylerhost.net/web"),
    ("Carson",     "https://carsoncountytx-web.tylerhost.net/web"),
    ("Dallam",     "https://dallamcountytx-web.tylerhost.net/web"),
    ("Donley",     "https://donleycountytx-web.tylerhost.net/web"),
    ("Winkler",    "https://winklercountytx-web.tylerhost.net/web"),
    ("Yoakum",     "https://yoakumcountytx-selfservice.tylerhost.net/web"),
    ("PaloPinto",  "https://palopintocountytx-selfservice.tylerhost.net/web"),
    ("Aransas",    "https://aransascountytx-web.tylerhost.net/web"),
]

lines = ["name: StackIQ Tyler Universal Scraper", "",
    "on:", "  schedule:", '    - cron: "0 4 * * *"', "  workflow_dispatch: {}", "", "jobs:"]

for county, base_url in COUNTIES:
    job_name = county.lower().replace(" ","_")
    fname = county.lower().replace(" ","_")
    lines += [
        f"  {job_name}:",
        "    runs-on: ubuntu-22.04",
        "    timeout-minutes: 120",
        "    steps:",
        "      - uses: actions/checkout@v4",
        "      - uses: actions/setup-python@v5",
        "        with:",
        '          python-version: "3.11"',
        "          cache: pip",
        "          cache-dependency-path: scraper/requirements.txt",
        "      - run: pip install -r scraper/requirements.txt",
        "      - run: playwright install chromium --with-deps",
        f"      - name: Scrape {county}",
        "        run: python scraper/tyler_universal.py",
        "        env:",
        f'          COUNTY_NAME: "{county}"',
        f'          BASE_URL: "{base_url}"',
        '          LOOKBACK_DAYS: "3"',
        "      - name: Push to DB",
        "        run: python scraper/push_to_db.py",
        "        env:",
        "          DATABASE_URL: ${{ secrets.DATABASE_URL }}",
        f'          JSON_GLOB: "dashboard/{fname}_records.json"',
        "        if: always()",
        "",
    ]

with open(".github/workflows/scrape_tyler_universal.yml", "w") as f:
    f.write("\n".join(lines))
print(f"Created workflow with {len(COUNTIES)} counties")
