import os, psycopg2
from datetime import datetime, timedelta

DB = os.environ.get("DATABASE_URL", "")
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "cmunoz@stackiq.org")

if not DB:
    print("No DATABASE_URL")
    exit(0)

conn = psycopg2.connect(DB, connect_timeout=30)
cur = conn.cursor()

# Get stats
cur.execute("SELECT COUNT(*) FROM lead_records")
total = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM lead_records WHERE scraped_at >= NOW() - INTERVAL '24 hours'")
new_today = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM lead_records WHERE score >= 70")
hot = cur.fetchone()[0]

cur.execute("SELECT county, COUNT(*) as cnt FROM lead_records WHERE scraped_at >= NOW() - INTERVAL '24 hours' GROUP BY county ORDER BY cnt DESC LIMIT 10")
new_by_county = cur.fetchall()

cur.execute("SELECT COUNT(DISTINCT county) FROM lead_records")
county_count = cur.fetchone()[0]

cur.close()
conn.close()

county_lines = "\n".join([f"  {c}: {n} new" for c, n in new_by_county]) or "  None"

body = f"""StackIQ Daily Scrape Report - {datetime.now().strftime('%Y-%m-%d')}

SUMMARY
-------
Total leads: {total:,}
New today: {new_today:,}
Hot leads (70+): {hot:,}
Active counties: {county_count}

NEW LEADS BY COUNTY (last 24h)
------------------------------
{county_lines}

View dashboard: https://stackiq.org/apps/underwriteiq/dashboard.html
"""

print(body)

if not SENDGRID_API_KEY:
    print("No SENDGRID_API_KEY - skipping email")
    exit(0)

import urllib.request, json
data = {
    "personalizations": [{"to": [{"email": ALERT_EMAIL}]}],
    "from": {"email": "alerts@stackiq.org", "name": "StackIQ"},
    "subject": f"StackIQ: {new_today} new leads today across {county_count} counties",
    "content": [{"type": "text/plain", "value": body}]
}
req = urllib.request.Request(
    "https://api.sendgrid.com/v3/mail/send",
    data=json.dumps(data).encode(),
    headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"},
    method="POST"
)
try:
    with urllib.request.urlopen(req) as resp:
        print(f"Email sent: {resp.status}")
except Exception as e:
    print(f"Email failed: {e}")
