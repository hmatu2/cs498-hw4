import pandas as pd

df = pd.read_csv("taxi_trips.csv")

cols = ["Trip ID", "Taxi ID", "Company", "Pickup Community Area",
        "Dropoff Community Area", "Fare", "Trip Seconds"]
df = df[cols]

df.columns = ["trip_id", "driver_id", "company", "pickup_area",
              "dropoff_area", "fare", "trip_seconds"]

df = df.dropna()

df["pickup_area"] = df["pickup_area"].astype(int)
df["dropoff_area"] = df["dropoff_area"].astype(int)
df["fare"] = df["fare"].astype(float)
df["trip_seconds"] = df["trip_seconds"].astype(int)

df = df[df["fare"] > 0]
df = df[df["trip_seconds"] > 0]

df = df.head(10000)

df.to_csv("taxi_trips_clean.csv", index=False)
print("done")
