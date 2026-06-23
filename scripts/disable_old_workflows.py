import re

for wf in [".github/workflows/scrape_bexar.yml", ".github/workflows/scrape_south.yml"]:
    with open(wf, encoding="utf-8") as f:
        content = f.read()
    # Remove the schedule cron line but keep workflow_dispatch
    content = re.sub(r'  schedule:\n    - cron:.*?\n', '', content)
    with open(wf, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Disabled schedule in {wf}")
