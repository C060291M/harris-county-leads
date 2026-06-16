import os, psycopg2, urllib.request, json
from datetime import datetime

DB = os.environ.get("DATABASE_URL", "")
API_URL = "https://api.stackiq.org"
API_SECRET = os.environ.get("API_SECRET", "")
ALERT_EMAIL = "cmunoz@stackiq.org"

if not DB:
    print("No DATABASE_URL"); exit(0)

conn = psycopg2.connect(DB, connect_timeout=30)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM lead_records")
total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM lead_records WHERE scraped_at >= NOW() - INTERVAL '24 hours'")
new_today = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM lead_records WHERE score >= 70")
hot = cur.fetchone()[0]
cur.execute("SELECT county, COUNT(*) as cnt, SUM(CASE WHEN score>=70 THEN 1 ELSE 0 END) as hot_cnt FROM lead_records WHERE scraped_at >= NOW() - INTERVAL '24 hours' GROUP BY county ORDER BY cnt DESC LIMIT 15")
rows = cur.fetchall()
cur.execute("SELECT COUNT(DISTINCT county) FROM lead_records")
county_count = cur.fetchone()[0]
cur.close()
conn.close()

county_rows = ""
for county, cnt, hot_cnt in rows:
    county_rows += f"""
    <tr>
      <td style="padding:8px 12px;border-bottom:1px solid #1e2d3d;color:#ffffff;font-weight:500">{county}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #1e2d3d;color:#ef4444;text-align:center">{int(hot_cnt or 0)}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #1e2d3d;color:#10b981;text-align:center">{cnt}</td>
    </tr>"""

body = f"""
<div style="background:#0f1c2e;color:#e2e8f0;font-family:Arial,sans-serif;max-width:600px;margin:0 auto;border-radius:12px;overflow:hidden">
  <div style="background:linear-gradient(135deg,#1e40af,#0f766e);padding:32px;text-align:center">
    <h1 style="margin:0;font-size:28px;font-weight:700">StackIQ</h1>
    <p style="margin:8px 0 0;opacity:0.8">Daily Lead Intelligence Report</p>
  </div>
  <div style="padding:24px">
    <div style="background:#1e2d3d;border-radius:8px;padding:20px;margin-bottom:20px">
      <p style="margin:0;font-size:16px">Good morning, <strong>Chris</strong> 👋</p>
      <p style="margin:8px 0 0;color:#94a3b8">Here's your daily lead report — <span style="color:#ef4444;font-weight:600">{hot:,} hot leads</span> and <span style="color:#10b981;font-weight:600">{new_today:,} new filings</span> across {county_count} counties.</p>
    </div>
    <h3 style="color:#94a3b8;font-size:11px;letter-spacing:1px;text-transform:uppercase;margin:0 0 12px">County Breakdown</h3>
    <table style="width:100%;border-collapse:collapse;background:#1e2d3d;border-radius:8px;overflow:hidden">
      <tr style="background:#162032">
        <th style="padding:10px 12px;text-align:left;font-size:11px;color:#64748b;text-transform:uppercase">County</th>
        <th style="padding:10px 12px;text-align:center;font-size:11px;color:#64748b;text-transform:uppercase">Hot (70+)</th>
        <th style="padding:10px 12px;text-align:center;font-size:11px;color:#64748b;text-transform:uppercase">New Today</th>
      </tr>
      {county_rows}
    </table>
    <div style="margin-top:24px;text-align:center">
      <a href="https://stackiq.org/apps/underwriteiq/dashboard.html" style="background:#10b981;color:#fff;padding:12px 32px;border-radius:8px;text-decoration:none;font-weight:600;display:inline-block">View Dashboard →</a>
    </div>
    <p style="margin:24px 0 0;color:#475569;font-size:12px;text-align:center">Total leads in database: {total:,}</p>
  </div>
</div>
"""

print(f"Total: {total:,} | New: {new_today:,} | Hot: {hot:,} | Counties: {county_count}")

if not API_SECRET:
    print("No API_SECRET - skipping email")
    exit(0)

try:
    data = json.dumps({"to_email": ALERT_EMAIL, "subject": f"StackIQ: {new_today:,} new leads | {hot:,} hot | {county_count} counties | {datetime.now().strftime('%m/%d/%Y')}", "body": body}).encode()
    req = urllib.request.Request(f"{API_URL}/api/send-email", data=data, headers={"Content-Type": "application/json", "Authorization": f"Bearer {API_SECRET}"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        print(f"Email sent: {resp.status}")
except Exception as e:
    print(f"Email failed: {e}")
