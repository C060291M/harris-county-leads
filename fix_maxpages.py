content = open(".github/workflows/scrape.yml", encoding="utf-8").read()

# Change default MAX_PAGES from 15 to 8 for all scraper jobs
content = content.replace(
    "MAX_PAGES: ${{ github.event.inputs.max_pages || '15' }}",
    "MAX_PAGES: ${{ github.event.inputs.max_pages || '8' }}"
)

open(".github/workflows/scrape.yml", "w", encoding="utf-8").write(content)
print("done")
