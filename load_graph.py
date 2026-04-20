import csv
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "password123"   # replace if you used a different password

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

def create_constraints(tx):
    tx.run("CREATE CONSTRAINT driver_id_unique IF NOT EXISTS FOR (d:Driver) REQUIRE d.driver_id IS UNIQUE")
    tx.run("CREATE CONSTRAINT company_name_unique IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS UNIQUE")
    tx.run("CREATE CONSTRAINT area_id_unique IF NOT EXISTS FOR (a:Area) REQUIRE a.area_id IS UNIQUE")

def load_row(tx, row):
    tx.run("""
        MERGE (d:Driver {driver_id: $driver_id})
        MERGE (c:Company {name: $company})
        MERGE (a:Area {area_id: $dropoff_area})
        MERGE (d)-[:WORKS_FOR]->(c)
        CREATE (d)-[:TRIP {
            trip_id: $trip_id,
            fare: $fare,
            trip_seconds: $trip_seconds
        }]->(a)
    """,
    trip_id=row["trip_id"],
    driver_id=row["driver_id"],
    company=row["company"],
    dropoff_area=int(row["dropoff_area"]),
    fare=float(row["fare"]),
    trip_seconds=int(row["trip_seconds"]))

def main():
    with driver.session() as session:
        session.execute_write(create_constraints)

        with open("taxi_trips_clean.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                session.execute_write(load_row, row)

    driver.close()
    print("loaded")

if __name__ == "__main__":
    main()
