# Create multi_county_travis.py (Travis solo)
content = open("scraper/multi_county_11.py", encoding="utf-8").read()

# Travis solo file
travis = content
travis = travis.replace(
    '"Travis": "travis.tx.publicsearch.us",\n    "Bell":   "bell.tx.publicsearch.us",',
    '"Travis": "travis.tx.publicsearch.us",'
)
travis = travis.replace("multi_county_11_records.json", "multi_county_travis_records.json")
open("scraper/multi_county_travis.py", "w", encoding="utf-8").write(travis)
print("Created multi_county_travis.py")

# mc11 Bell only
bell = content.replace(
    '"Travis": "travis.tx.publicsearch.us",\n    "Bell":   "bell.tx.publicsearch.us",',
    '"Bell": "bell.tx.publicsearch.us",'
)
open("scraper/multi_county_11.py", "w", encoding="utf-8").write(bell)
print("Updated multi_county_11.py to Bell only")
