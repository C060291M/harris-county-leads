import psycopg2, os

conn = psycopg2.connect(os.environ["DATABASE_URL"])
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS scraper_runs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(50),
    county VARCHAR(100),
    run_at TIMESTAMP DEFAULT NOW(),
    leads_pushed INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'success',
    error_msg TEXT
)
""")
print("Created scraper_runs table")

cur.execute("CREATE INDEX IF NOT EXISTS idx_scraper_runs_county ON scraper_runs(county)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_scraper_runs_run_at ON scraper_runs(run_at)")
print("Indexes created")
conn.close()
