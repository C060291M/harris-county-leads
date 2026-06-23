import re
for wf in [".github/workflows/scrape_bexar.yml", ".github/workflows/scrape_south.yml"]:
    with open(wf, encoding="utf-8") as f:
        content = f.read()
    # Remove schedule block entirely
    content = re.sub(r'  schedule:\n    - cron:[^\n]+\n', '', content)
    with open(wf, "w", encoding="utf-8") as f:
        f.write(content)
    has_schedule = "cron:" in content
    print(f"{wf}: schedule={'STILL PRESENT' if has_schedule else 'REMOVED'}")
