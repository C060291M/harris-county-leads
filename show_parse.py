with open('scraper/multi_county_galveston.py', encoding='utf-8') as f:
    lines = f.readlines()

# Find parse_results function and show it
start = None
for i, line in enumerate(lines):
    if 'def parse_results' in line:
        start = i
        break

if start:
    print("parse_results function:")
    for line in lines[start:start+50]:
        print(line, end='')
