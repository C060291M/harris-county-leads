#!/usr/bin/env python3
r"""
generate_tyler_workflows.py
Run from harris-county-leads repo root to regenerate all scrape_tyler_*.yml
using the new universal tyler_scraper_universal.py.

Each county gets its own parallel job. Counties are distributed across
workflow files (a-f) to stay under GitHub's 256-job limit per workflow.

Run:
    cd C:\Users\cmuno\OneDrive\Desktop\harris-county-leads
    python generate_tyler_workflows.py
"""

import os, math

# All 59 Tyler counties in order — split across 6 workflow files
ALL_COUNTIES = [
    # tyler_a
    "Rockwall", "Kaufman", "Delta", "Hood", "Henderson", "Hunt",
    "Navarro", "VanZandt", "Hill", "Lamar",
    # tyler_b
    "Gregg", "Harrison", "Upshur", "Wood", "Eastland", "Erath",
    "Liberty", "Hardin", "Orange", "Jasper",
    # tyler_c
    "Hays", "Comal", "Guadalupe", "Bastrop", "Burnet", "Gonzales",
    "Karnes", "Calhoun", "Wichita", "Howard",
    # tyler_d
    "Potter", "Andrews", "PaloPinto", "Wise", "Bowie", "Polk",
    "Winkler", "Yoakum", "Somervell", "Hamilton", "Mills", "McLennan",
    # tyler_e
    "Ector", "Taylor", "Randall", "Lavaca", "Calhoun2",
    "Panola", "Scurry", "Washington", "Pecos", "Dallam",
    # tyler_f
    "Aransas", "Carson", "Colorado", "Donley", "Kimble",
    "Waller", "Williamson", "Montgomery", "Fort Bend", "Brazoria",
]

GROUPS = {
    "a": ALL_COUNTIES[0:10],
    "b": ALL_COUNTIES[10:20],
    "c": ALL_COUNTIES[20:30],
    "d": ALL_COUNTIES[30:42],
    "e": ALL_COUNTIES[42:52],
    "f": ALL_COUNTIES[52:],
}

# Cron times (CDT = UTC-5, so 3am CDT = 8am UTC)
CRON = "30 8 * * *"   # 3:30am CDT


def job_name(county):
    return county.lower().replace(" ", "-").replace("_", "-")


def make_workflow(group_letter, counties):
    jobs = []
    for county in counties:
        jname = job_name(county)
        job = f"""
  {jname}:
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
      - name: Scrape {county}
        run: python scraper/tyler_scraper_universal.py
        env:
          COUNTY: "{county}"
          LOOKBACK_DAYS: "3"
          MAX_PAGES: "5"
          MAX_RECORDS: "500"
          WALL_MINUTES: "75"
      - name: Push to DB
        run: python scraper/push_to_db.py
        env:
          DATABASE_URL: ${{{{ secrets.DATABASE_URL }}}}"""
        jobs.append(job)

    return f"""name: StackIQ Tyler iDS Group {group_letter.upper()}

on:
  schedule:
    - cron: "{CRON}"
  workflow_dispatch: {{}}

jobs:
{"".join(jobs)}
"""


def main():
    wf_dir = os.path.join(".github", "workflows")
    os.makedirs(wf_dir, exist_ok=True)

    for letter, counties in GROUPS.items():
        content = make_workflow(letter, counties)
        path = os.path.join(wf_dir, f"scrape_tyler_{letter}.yml")
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(content)
        print(f"scrape_tyler_{letter}.yml: {len(counties)} counties")

    print(f"\nTotal: {sum(len(v) for v in GROUPS.values())} counties across 6 workflows")
    print("Next: git add .github/workflows/ scraper/tyler_scraper_universal.py && git commit")


if __name__ == "__main__":
    main()
