import os, psycopg2, smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

DB = os.environ.get("DATABASE_URL", "")
GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_PASS = os.environ.get("GMAIL_PASS", "")
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "cmunoz@stackiq.org")

if not DB:
    print("No DATABASE_URL")
    exit(0)

conn = psycopg2.connect(DB, connect_timeout=30)
cur = conn.cursor()

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
New today:   {new_today:,}
Hot leads:   {hot:,}
Counties:    {county_count}

NEW LEADS BY COUNTY (last 24h)
------------------------------
{county_lines}

View dashboard: https://stackiq.org/apps/underwriteiq/dashboard.html
"""

print(body)

if not GMAIL_USER or not GMAIL_PASS:
    print("No GMAIL credentials - skipping email")
    exit(0)

try:
    msg = MIMEMultipart()
    msg['From'] = GMAIL_USER
    msg['To'] = ALERT_EMAIL
    msg['Subject'] = f"StackIQ: {new_today:,} new leads | {county_count} counties | {datetime.now().strftime('%m/%d/%Y')}"
    msg.attach(MIMEText(body, 'plain'))
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(GMAIL_USER, GMAIL_PASS)
        server.sendmail(GMAIL_USER, ALERT_EMAIL, msg.as_string())
    print(f"Email sent to {ALERT_EMAIL}")
except Exception as e:
    print(f"Email failed: {e}")
