"""
scripts/generate_mock_data.py
-------------------------------
Generates realistic, Bengaluru-centric mock data for the Urban Mobility
Intelligence Platform, deliberately injecting data quality problems so
every Silver validation rule has real bad data to catch.

Bad-data patterns injected (one for every validation rule in Silver):
    - Null primary keys (drivers, trips)
    - Duplicate primary keys (drivers, trips)
    - Duplicate customer IDs (source.customers has no PK constraint --
      models a real upstream export that isn't perfectly deduplicated)
    - Negative fares
    - Invalid / impossible driver ratings (7.5, -1, 11.0)
    - Invalid payment amounts (null, zero, negative)
    - Mismatched trip timestamps (drop_time before request_time)
    - Missing driver_id / vehicle_id on some trips
    - Cancelled rides that still have a payment record (shouldn't happen)
    - Completed rides missing a payment record (shouldn't happen)
    - Corrupted JSON rows (trips.json is JSON Lines format -- one JSON
      object per line -- so a handful of lines can be deliberately broken
      without corrupting the rest of the file; see bronze_ingestion.py's
      PERMISSIVE-mode JSON reader)
    - Malformed XML records (empty <vehicle/> elements with no child
      fields -- the file is still well-formed XML overall, but individual
      <vehicle> records are unusable)

Volumes are configurable via environment variables so the same script
scales from a fast local smoke test to the platform's full enterprise
target. Defaults below match the target volumes.

Sources produced:
    CSV      -> drivers, payments, traffic, weather, driver_shift
    JSON     -> trips (JSON Lines format, see above)
    XML      -> vehicles
    Postgres -> customers (source.customers)

Run: python3 scripts/generate_mock_data.py
     UMIP_N_TRIPS=5000 python3 scripts/generate_mock_data.py   # smaller/faster run
"""
import csv
import json
import os
import random
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker("en_IN")
random.seed(7)
Faker.seed(7)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_ROOT = os.path.join(PROJECT_ROOT, "data", "raw")

# Bengaluru is the anchor city; a few others included so city-level KPIs are meaningful
CITIES = ["Bengaluru", "Bengaluru", "Bengaluru", "Mumbai", "Hyderabad"]
BLR_ZONES = ["Koramangala", "Indiranagar", "Whitefield", "HSR Layout",
             "Electronic City", "Marathahalli", "Airport", "Jayanagar", "MG Road"]
OTHER_ZONES = {"Mumbai": ["Andheri", "Bandra", "Powai", "Airport"],
               "Hyderabad": ["Hitech City", "Gachibowli", "Airport"]}

VEHICLE_TYPES = ["Hatchback", "Sedan", "SUV", "Auto", "Bike"]
PAYMENT_MODES = ["UPI", "Card", "Cash", "Wallet"]

# Configurable volumes -- defaults match the platform's enterprise target.
# Override any of these with an env var for a faster local smoke test,
# e.g. UMIP_N_TRIPS=5000.
N_DRIVERS = int(os.environ.get("UMIP_N_DRIVERS", 1000))
N_CUSTOMERS = int(os.environ.get("UMIP_N_CUSTOMERS", 10000))
N_VEHICLES = int(os.environ.get("UMIP_N_VEHICLES", 500))
N_TRIPS = int(os.environ.get("UMIP_N_TRIPS", 110000))      # ~90% complete -> ~100k completed trips/payments
N_SHIFTS = int(os.environ.get("UMIP_N_SHIFTS", 15000))     # sampled across a 365-day window
N_TRAFFIC = int(os.environ.get("UMIP_N_TRAFFIC", 500))
N_WEATHER = int(os.environ.get("UMIP_N_WEATHER", 365))     # one record per day for a year
SHIFT_WINDOW_DAYS = 365

POSTGRES_CONFIG = dict(
    host=os.environ.get("UMIP_PG_HOST", "localhost"),
    port=int(os.environ.get("UMIP_PG_PORT", 5432)),
    dbname=os.environ.get("UMIP_PG_DB", "umip_platform"),
    user=os.environ.get("UMIP_PG_USER", "umip_admin"),
    password=os.environ.get("UMIP_PG_PASSWORD", "umip_password"),
)


def zone_for_city(city):
    return random.choice(BLR_ZONES) if city == "Bengaluru" else random.choice(OTHER_ZONES[city])


def rand_time_within(days_back=45):
    return datetime.now() - timedelta(
        days=random.randint(0, days_back), hours=random.randint(0, 23), minutes=random.randint(0, 59)
    )


def ensure_dirs():
    for sub in ["drivers", "payments", "traffic", "weather", "driver_shift", "trips", "vehicles"]:
        os.makedirs(os.path.join(RAW_ROOT, sub), exist_ok=True)


# ---------------------------------------------------------------------------
# DRIVERS (CSV) — includes null PKs, extreme ratings, duplicates
# ---------------------------------------------------------------------------
def generate_drivers():
    rows = []
    for i in range(1, N_DRIVERS + 1):
        rating = round(random.uniform(3.0, 5.0), 2)
        # inject extreme/impossible ratings for ~2% of rows
        if random.random() < 0.02:
            rating = random.choice([7.5, -1.0, 11.0])
        rows.append({
            "driver_id": i,
            "driver_name": fake.name(),
            "phone_number": fake.phone_number(),
            "city": "Bengaluru" if random.random() < 0.6 else random.choice(CITIES),
            "rating": rating,
            "joined_date": fake.date_between(start_date="-3y", end_date="-30d").isoformat(),
            "status": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE", "SUSPENDED"]),
            "updated_at": rand_time_within(10).strftime("%Y-%m-%d %H:%M:%S"),
        })

    # inject null primary keys (edge case) -- ~0.5% of volume, min 2 rows
    n_null_pk = max(2, int(N_DRIVERS * 0.005))
    for _ in range(n_null_pk):
        bad = dict(random.choice(rows))
        bad["driver_id"] = random.choice([None, ""])
        rows.append(bad)

    # inject duplicate primary keys -- ~0.5% of volume, min 2 rows
    n_dupes = max(2, int(N_DRIVERS * 0.005))
    for _ in range(n_dupes):
        rows.append(dict(random.choice(rows[:N_DRIVERS])))

    path = os.path.join(RAW_ROOT, "drivers", "drivers.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


# ---------------------------------------------------------------------------
# VEHICLES (XML) — legacy vendor system style. Includes a few malformed
# <vehicle> records (empty elements, no child fields) so the per-record
# resilient XML parser in bronze_ingestion.py has something real to catch.
# ---------------------------------------------------------------------------
def generate_vehicles(driver_ids):
    rows = []
    for i in range(1, N_VEHICLES + 1):
        rows.append({
            "vehicle_id": i,
            "driver_id": random.choice(driver_ids),
            "vehicle_type": random.choice(VEHICLE_TYPES),
            "registration_number": fake.bothify(text="KA-##-??-####").upper(),
            "make": random.choice(["Maruti", "Hyundai", "Toyota", "Tata", "Honda", "Mahindra"]),
            "model_year": random.randint(2014, 2025),
            "is_active": random.random() > 0.05,
        })

    lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<vehicles>"]
    for v in rows:
        lines.append("  <vehicle>")
        for k, val in v.items():
            lines.append(f"    <{k}>{val}</{k}>")
        lines.append("  </vehicle>")

    # malformed records: empty <vehicle/> elements with no child fields at
    # all. Document stays well-formed XML overall; each of these individual
    # records is unusable and gets caught + skipped by bronze_ingestion.py.
    n_malformed = max(2, int(N_VEHICLES * 0.01))
    for _ in range(n_malformed):
        lines.append("  <vehicle></vehicle>")

    lines.append("</vehicles>")

    path = os.path.join(RAW_ROOT, "vehicles", "vehicles.xml")
    with open(path, "w") as f:
        f.write("\n".join(lines))
    return rows


# ---------------------------------------------------------------------------
# TRIPS (JSON Lines) — one JSON object per line, NOT a single JSON array.
# This is what lets us corrupt a handful of individual lines without
# breaking the rest of the file: a broken line in a JSON-array file would
# fail the ENTIRE array's parse, but a broken line in JSON Lines format
# only fails that one line (see bronze_ingestion.py's PERMISSIVE reader).
# ---------------------------------------------------------------------------
def generate_trips(driver_ids, customer_ids, vehicle_ids):
    rows = []
    for i in range(1, N_TRIPS + 1):
        city = random.choice(CITIES)
        pickup_zone = zone_for_city(city)
        drop_zone = zone_for_city(city)
        request_time = rand_time_within(45)
        trip_minutes = random.randint(5, 75)
        drop_time = request_time + timedelta(minutes=trip_minutes)
        is_cancelled = random.random() < 0.1

        fare = None if is_cancelled else round(random.uniform(80, 1400), 2)
        # inject negative fares for ~1.5% of completed trips
        if fare is not None and random.random() < 0.015:
            fare = -abs(fare)

        row = {
            "trip_id": i,
            "driver_id": random.choice(driver_ids),
            "customer_id": random.choice(customer_ids),
            "vehicle_id": random.choice(vehicle_ids),
            "city": city,
            "pickup_zone": pickup_zone,
            "drop_zone": drop_zone,
            "request_time": request_time.strftime("%Y-%m-%d %H:%M:%S"),
            "drop_time": drop_time.strftime("%Y-%m-%d %H:%M:%S"),
            "trip_date": request_time.strftime("%Y-%m-%d"),
            "trip_status": "CANCELLED" if is_cancelled else "COMPLETED",
            "distance_km": None if is_cancelled else round(random.uniform(1, 32), 2),
            "fare_amount": fare,
        }
        rows.append(row)

    # missing driver_id / vehicle_id on a small % of trips (required-field nulls)
    n_missing_driver = max(2, int(N_TRIPS * 0.003))
    for _ in range(n_missing_driver):
        random.choice(rows)["driver_id"] = None
    n_missing_vehicle = max(2, int(N_TRIPS * 0.003))
    for _ in range(n_missing_vehicle):
        random.choice(rows)["vehicle_id"] = None

    # mismatched timestamps: drop_time BEFORE request_time (data bug)
    n_mismatched = max(3, int(N_TRIPS * 0.001))
    for _ in range(n_mismatched):
        bad = dict(random.choice(rows))
        req = datetime.strptime(bad["request_time"], "%Y-%m-%d %H:%M:%S")
        bad["drop_time"] = (req - timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S")
        rows.append(bad)

    # null primary keys
    n_null_pk = max(2, int(N_TRIPS * 0.001))
    for _ in range(n_null_pk):
        bad_pk = dict(random.choice(rows))
        bad_pk["trip_id"] = None
        rows.append(bad_pk)

    # duplicate trip_id
    n_dupes = max(2, int(N_TRIPS * 0.001))
    for _ in range(n_dupes):
        rows.append(dict(random.choice(rows[:N_TRIPS])))

    path = os.path.join(RAW_ROOT, "trips", "trips.json")
    n_corrupt = max(3, int(N_TRIPS * 0.0005))
    corrupt_line_positions = set(random.sample(range(len(rows)), min(n_corrupt, len(rows))))

    with open(path, "w") as f:
        for idx, row in enumerate(rows):
            if idx in corrupt_line_positions:
                # deliberately broken JSON syntax: unbalanced braces / stray
                # text. PERMISSIVE mode in bronze_ingestion.py catches this
                # per-line without failing the rest of the file.
                f.write('{"trip_id": %s, "driver_id": BROKEN_SYNTAX,,, }\n' % row.get("trip_id", "null"))
            else:
                f.write(json.dumps(row, default=str) + "\n")

    return rows


# ---------------------------------------------------------------------------
# PAYMENTS (CSV) — one per completed trip, plus deliberate mismatches:
# some completed trips get NO payment, and a few cancelled trips get one
# anyway (both are real operational data-quality problems).
# ---------------------------------------------------------------------------
def generate_payments(trips):
    rows = []
    pid = 1

    completed = [t for t in trips if t["trip_status"] == "COMPLETED" and t.get("fare_amount") and t["trip_id"] is not None]
    cancelled = [t for t in trips if t["trip_status"] == "CANCELLED" and t["trip_id"] is not None]

    # completed ride WITHOUT payment: skip ~1% of completed trips
    n_missing_payment = max(3, int(len(completed) * 0.01))
    skip_ids = set(t["trip_id"] for t in random.sample(completed, min(n_missing_payment, len(completed))))

    for t in completed:
        if t["trip_id"] in skip_ids:
            continue
        rows.append({
            "payment_id": pid,
            "trip_id": t["trip_id"],
            "amount": t["fare_amount"],
            "payment_mode": random.choice(PAYMENT_MODES),
            "payment_status": random.choices(["SUCCESS", "FAILED"], weights=[96, 4])[0],
            "payment_date": t["trip_date"],
            "payment_time": t["request_time"],
        })
        pid += 1

    # cancelled ride WITH a payment record (shouldn't happen): ~0.5%
    n_bad_cancelled_payment = max(2, int(len(cancelled) * 0.005))
    for t in random.sample(cancelled, min(n_bad_cancelled_payment, len(cancelled))):
        rows.append({
            "payment_id": pid,
            "trip_id": t["trip_id"],
            "amount": round(random.uniform(80, 500), 2),
            "payment_mode": random.choice(PAYMENT_MODES),
            "payment_status": "SUCCESS",
            "payment_date": t["trip_date"],
            "payment_time": t["request_time"],
        })
        pid += 1

    # invalid payment amounts: null and zero/negative
    if len(rows) > 20:
        rows[5]["amount"] = ""
        rows[20]["amount"] = ""
    n_invalid_amount = max(3, int(len(rows) * 0.005))
    for _ in range(n_invalid_amount):
        random.choice(rows)["amount"] = random.choice([0, -50.0])

    path = os.path.join(RAW_ROOT, "payments", "payments.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


# ---------------------------------------------------------------------------
# DRIVER SHIFT (CSV) — sampled across a 365-day window. Note: overlapping
# shifts for the same driver can occur naturally here (two shift rows for
# the same driver with overlapping start/end times) -- this is real-world
# messy data, but there is currently no cross-record validation rule in
# Silver that checks for shift overlap (that requires a window-based
# temporal-overlap check across rows, not a single-row rule). It's called
# out explicitly in docs/17_Testing_Guide.md as a known gap / suggested
# future improvement rather than silently ignored.
# ---------------------------------------------------------------------------
def generate_driver_shift(driver_ids):
    rows = []
    for i in range(1, N_SHIFTS + 1):
        start = rand_time_within(SHIFT_WINDOW_DAYS)
        hours = random.choice([4, 6, 8, 10, 12])
        rows.append({
            "shift_id": i,
            "driver_id": random.choice(driver_ids),
            "shift_date": start.strftime("%Y-%m-%d"),
            "shift_start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
            "shift_end_time": (start + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S"),
            "online_hours": hours,
        })
    path = os.path.join(RAW_ROOT, "driver_shift", "driver_shift.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


# ---------------------------------------------------------------------------
# TRAFFIC (CSV)
# ---------------------------------------------------------------------------
def generate_traffic():
    rows = []
    for i in range(1, N_TRAFFIC + 1):
        city = random.choice(CITIES)
        zone = zone_for_city(city)
        rows.append({
            "traffic_id": i,
            "city": city,
            "zone_name": zone,
            "congestion_level": random.choice(["LOW", "MODERATE", "HIGH", "SEVERE"]),
            "avg_speed_kmph": round(random.uniform(8, 45), 1),
            "recorded_at": rand_time_within(30).strftime("%Y-%m-%d %H:%M:%S"),
        })
    path = os.path.join(RAW_ROOT, "traffic", "traffic.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


# ---------------------------------------------------------------------------
# WEATHER (CSV) — one record per day for a year
# ---------------------------------------------------------------------------
def generate_weather():
    rows = []
    for i in range(1, N_WEATHER + 1):
        city = random.choice(CITIES)
        rows.append({
            "weather_id": i,
            "city": city,
            "weather_date": (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d"),
            "condition": random.choice(["Clear", "Rain", "Heavy Rain", "Cloudy", "Fog"]),
            "temperature_c": round(random.uniform(18, 36), 1),
            "rainfall_mm": round(random.uniform(0, 55), 1),
        })
    path = os.path.join(RAW_ROOT, "weather", "weather.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


# ---------------------------------------------------------------------------
# CUSTOMERS -> loaded directly into Postgres source.customers.
# Batched with execute_values instead of one INSERT per row -- matters at
# 10,000 rows. Includes a small number of deliberate duplicate customer_id
# rows (source.customers has no PK constraint, see db_setup.sql).
# ---------------------------------------------------------------------------
def load_customers_to_postgres():
    """
    Loads customer master data into PostgreSQL.

    This table is the master Customer dimension and therefore
    customer_id must remain unique.

    The table is truncated before every run so the script is
    completely rerunnable without duplicate-key failures.
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    # Clean existing data for a fresh run
    cur.execute("TRUNCATE TABLE source.customers RESTART IDENTITY;")

    customer_ids = []
    values = []

    for i in range(1, N_CUSTOMERS + 1):

        values.append((
            i,
            fake.name(),
            fake.phone_number(),
            fake.email() if random.random() > 0.02 else None,
            "Bengaluru" if random.random() < 0.6 else random.choice(CITIES),
            fake.date_between(start_date="-3y", end_date="-10d"),
            random.random() < 0.15,
            rand_time_within(10)
        ))

        customer_ids.append(i)

    execute_values(
        cur,
        """
        INSERT INTO source.customers
        (
            customer_id,
            customer_name,
            phone_number,
            email,
            city,
            signup_date,
            is_premium,
            updated_at
        )
        VALUES %s
        """,
        values,
        page_size=1000
    )

    cur.close()
    conn.close()

    print(f"Loaded {len(customer_ids)} unique customers into PostgreSQL.")

    return customer_ids

    # duplicate customer IDs: ~0.3% of volume, min 3 -- re-insert a handful
    # of existing customer_ids with fresh (different) attribute values,
    # simulating an upstream export that wasn't deduplicated
   # n_dupe_customers = max(3, int(N_CUSTOMERS * 0.003))
    #for _ in range(n_dupe_customers):
     #   dupe_id = random.choice(customer_ids)
      #  values.append((
       #     dupe_id, fake.name(), fake.phone_number(), fake.email(),
        #    random.choice(CITIES), fake.date_between(start_date="-3y", end_date="-10d"),
         #   random.random() < 0.15, rand_time_within(10),
        #))

    execute_values(
        cur,
        """INSERT INTO source.customers
           (customer_id, customer_name, phone_number, email, city, signup_date, is_premium, updated_at)
           VALUES %s""",
        values, page_size=1000,
    )

    cur.close()
    conn.close()
    print(f"Loaded {len(values)} customer rows into Postgres source.customers ({len(customer_ids)} unique IDs, {n_dupe_customers} deliberate duplicates)")
    return customer_ids


def main():
    ensure_dirs()
    print("Generating mock data for Urban Mobility Intelligence Platform...")
    print(f"Volumes: drivers={N_DRIVERS} customers={N_CUSTOMERS} vehicles={N_VEHICLES} "
          f"trips={N_TRIPS} shifts={N_SHIFTS} traffic={N_TRAFFIC} weather={N_WEATHER}")

    drivers = generate_drivers()
    driver_ids = [d["driver_id"] for d in drivers if d["driver_id"] not in (None, "")]

    customer_ids = load_customers_to_postgres()

    vehicles = generate_vehicles(driver_ids)
    vehicle_ids = [v["vehicle_id"] for v in vehicles]

    trips = generate_trips(driver_ids, customer_ids, vehicle_ids)
    payments = generate_payments(trips)
    shifts = generate_driver_shift(driver_ids)
    traffic = generate_traffic()
    weather = generate_weather()

    print("Done. Record counts:")
    print(f"  drivers={len(drivers)} (raw, incl. dirty rows)")
    print(f"  customers={len(customer_ids)} unique (in Postgres, incl. duplicates)")
    print(f"  vehicles={len(vehicles)}")
    print(f"  trips={len(trips)} (raw, incl. dirty rows)")
    print(f"  payments={len(payments)}")
    print(f"  driver_shift={len(shifts)}")
    print(f"  traffic={len(traffic)}")
    print(f"  weather={len(weather)}")


if __name__ == "__main__":
    main()
