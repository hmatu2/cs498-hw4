from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round

app = Flask(__name__)


NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"

neo4j_driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

spark = SparkSession.builder.appName("HW4App").getOrCreate()
CSV_PATH = "taxi_trips_clean.csv"


@app.route("/graph-summary", methods=["GET"])
def graph_summary():
    query = """
    CALL {
        MATCH (d:Driver)
        RETURN count(d) AS driver_count
    }
    CALL {
        MATCH (c:Company)
        RETURN count(c) AS company_count
    }
    CALL {
        MATCH (a:Area)
        RETURN count(a) AS area_count
    }
    CALL {
        MATCH ()-[t:TRIP]->()
        RETURN count(t) AS trip_count
    }
    RETURN driver_count, company_count, area_count, trip_count
    """
    with neo4j_driver.session() as session:
        record = session.run(query).single()
        return jsonify({
            "driver_count": record["driver_count"],
            "company_count": record["company_count"],
            "area_count": record["area_count"],
            "trip_count": record["trip_count"]
        })

@app.route("/top-companies", methods=["GET"])
def top_companies():
    n = int(request.args.get("n", 5))

    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[t:TRIP]->(:Area)
    RETURN c.name AS name, count(t) AS trip_count
    ORDER BY trip_count DESC
    LIMIT $n
    """
    with neo4j_driver.session() as session:
        results = session.run(query, n=n)
        companies = [{"name": r["name"], "trip_count": r["trip_count"]} for r in results]
        return jsonify({"companies": companies})

@app.route("/high-fare-trips", methods=["GET"])
def high_fare_trips():
    area_id = int(request.args.get("area_id"))
    min_fare = float(request.args.get("min_fare"))

    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
    WHERE t.fare > $min_fare
    RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
    ORDER BY fare DESC
    """
    with neo4j_driver.session() as session:
        results = session.run(query, area_id=area_id, min_fare=min_fare)
        trips = [
            {
                "trip_id": r["trip_id"],
                "fare": float(r["fare"]),
                "driver_id": r["driver_id"]
            }
            for r in results
        ]
        return jsonify({"trips": trips})

@app.route("/co-area-drivers", methods=["GET"])
def co_area_drivers():
    driver_id = request.args.get("driver_id")

    query = """
    MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
    WHERE d1 <> d2
    RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
    ORDER BY shared_areas DESC, driver_id ASC
    """
    with neo4j_driver.session() as session:
        results = session.run(query, driver_id=driver_id)
        co_drivers = [{"driver_id": r["driver_id"], "shared_areas": r["shared_areas"]} for r in results]
        return jsonify({"co_area_drivers": co_drivers})

@app.route("/avg-fare-by-company", methods=["GET"])
def avg_fare_by_company():
    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[t:TRIP]->(:Area)
    RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
    ORDER BY avg_fare DESC
    """
    with neo4j_driver.session() as session:
        results = session.run(query)
        companies = [{"name": r["name"], "avg_fare": float(r["avg_fare"])} for r in results]
        return jsonify({"companies": companies})

@app.route("/area-stats", methods=["GET"])
def area_stats():
    area_id = int(request.args.get("area_id"))
    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

    result = (
        df.filter(col("dropoff_area") == area_id)
          .groupBy("dropoff_area")
          .agg(
              count("*").alias("trip_count"),
              spark_round(avg("fare"), 2).alias("avg_fare"),
              spark_round(avg("trip_seconds"), 0).cast("int").alias("avg_trip_seconds")
          )
          .collect()
    )

    if not result:
        return jsonify({
            "area_id": area_id,
            "trip_count": 0,
            "avg_fare": 0.0,
            "avg_trip_seconds": 0
        })

    row = result[0]
    return jsonify({
        "area_id": area_id,
        "trip_count": row["trip_count"],
        "avg_fare": float(row["avg_fare"]),
        "avg_trip_seconds": int(row["avg_trip_seconds"])
    })

@app.route("/top-pickup-areas", methods=["GET"])
def top_pickup_areas():
    n = int(request.args.get("n", 5))
    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

    rows = (
        df.groupBy("pickup_area")
          .agg(count("*").alias("trip_count"))
          .orderBy(col("trip_count").desc(), col("pickup_area").asc())
          .limit(n)
          .collect()
    )

    areas = [{"pickup_area": row["pickup_area"], "trip_count": row["trip_count"]} for row in rows]
    return jsonify({"areas": areas})

@app.route("/company-compare", methods=["GET"])
def company_compare():
    company1 = request.args.get("company1")
    company2 = request.args.get("company2")

    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))
    df.createOrReplaceTempView("trips")

    query = f"""
    SELECT
        company,
        COUNT(*) AS trip_count,
        ROUND(AVG(fare), 2) AS avg_fare,
        ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
        CAST(ROUND(AVG(trip_seconds), 0) AS INT) AS avg_trip_seconds
    FROM trips
    WHERE company IN ('{company1}', '{company2}')
    GROUP BY company
    ORDER BY company
    """

    rows = spark.sql(query).collect()

    if len(rows) < 2:
        return jsonify({"error": "one or more companies not found"})

    comparison = []
    for row in rows:
        comparison.append({
            "company": row["company"],
            "trip_count": row["trip_count"],
            "avg_fare": float(row["avg_fare"]),
            "avg_fare_per_minute": float(row["avg_fare_per_minute"]),
            "avg_trip_seconds": int(row["avg_trip_seconds"])
        })

    return jsonify({"comparison": comparison})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
