with open('.github/workflows/scrape_tyler_universal.yml') as f:
    content = f.read()

# Find and remove wichita job
import re
new_content = re.sub(r'  wichita:\n.*?(?=  \w|\Z)', '', content, flags=re.DOTALL)

with open('.github/workflows/scrape_tyler_universal.yml', 'w') as f:
    f.write(new_content)
print('Wichita removed from universal workflow')
