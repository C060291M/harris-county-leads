import csv
import psycopg2
from psycopg2.extras import execute_values
import os

conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=30)
cur = conn.cursor()

print("Loading Owner file...")
owner_map = {}
with open(r"fortbend_export\owner\PropertyDataExport4111497.txt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter="\t")
    header = next(reader)
    idx = {name: i for i, name in enumerate(header)}
    for row in reader:
        try:
            pid = row[idx["PropertyID"]]
            owner_map[pid] = row[idx["OwnerName"]]
        except Exception:
            continue
print(f"  {len(owner_map)} owners loaded")

print("Loading Segment file (MA rows only)...")
segment_map = {}
with open(r"fortbend_export\segment\PropertyDataExport4111501.txt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter=",", quotechar='"')
    header = next(reader)
    idx = {name: i for i, name in enumerate(header)}
    for row in reader:
        try:
            if row[idx["Type"]] != "MA":
                continue
            pid = row[idx["PropertyID"]]
            yr = row[idx["ActYrBuilt"]]
            area = row[idx["Area"]]
            beds = row[idx["Bedrooms"]]
            segment_map[pid] = (yr, area, beds)
        except Exception:
            continue
print(f"  {len(segment_map)} main-area segments loaded")

print("Streaming Property file and joining...")
batch = []
total = 0
with open(r"fortbend_export\property\PropertyDataExport4111495.txt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter=",", quotechar='"')
    header = next(reader)
    idx = {name: i for i, name in enumerate(header)}
    for row in reader:
        try:
            pid = row[idx["PropertyID"]]
            street_num = row[idx["SitusStreetNumber"]]
            street_name = row[idx["SitusStreetName"]]
            street_suffix = row[idx["SitusStreetSuffix"]]
            city = row[idx["SitusCity"]]
            state = row[idx["SitusState"]]
            zip_ = row[idx["SitusZip"]]
            if not street_num or not street_name:
                continue
            situs = f"{street_num} {street_name} {street_suffix}, {city} {state} {zip_}".strip()

            owner_name = owner_map.get(pid)
            if not owner_name:
                continue
            seg = segment_map.get(pid, (None, None, None))

            batch.append((pid, owner_name, situs, seg[0], seg[1], seg[2]))
            if len(batch) >= 5000:
                execute_values(cur, "INSERT INTO fortbend_cad (property_id, owner_name, situs_address, yr_built, living_area, bedrooms) VALUES %s", batch)
                conn.commit()
                total += len(batch)
                print(f"  {total} rows imported so far...")
                batch = []
        except Exception:
            continue

if batch:
    execute_values(cur, "INSERT INTO fortbend_cad (property_id, owner_name, situs_address, yr_built, living_area, bedrooms) VALUES %s", batch)
    conn.commit()
    total += len(batch)

print(f"DONE: {total} rows imported into fortbend_cad")
