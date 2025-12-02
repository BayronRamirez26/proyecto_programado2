from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum, count, when, round, hour, date_format,
    current_timestamp, to_date, lit, max
)
import os
import sys

print("Initializing Spark Session...")

# Configuración del Classpath para el driver JDBC de PostgreSQL
spark = SparkSession.builder \
    .appName("TransportAnalyticsPipeline") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()
    
# Parámetros de conexión a PostgreSQL
POSTGRES_PROPERTIES = {
    "user": "transport_user",
    "password": "transport_pass",
    "driver": "org.postgresql.Driver"
}
POSTGRES_URL = "jdbc:postgresql://postgres:5432/transport_db"

# Ruta de los datos procesados
DATA_PATH = "/data"

# Función de Carga (Load)
def load_to_postgres(df, schema_name, table_name, mode="overwrite"):
    full_table_name = f"{schema_name}.{table_name}"
    print(f"Loading data to PostgreSQL table: {full_table_name}")
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", full_table_name) \
        .options(**POSTGRES_PROPERTIES) \
        .mode(mode) \
        .save()
    print(f"Finished loading {df.count()} rows into {full_table_name}.")


# Lectura de Datos Base (Extract)
print("\nAttempting to read CSV files...")
try:
    transport_df = spark.read.csv(os.path.join(DATA_PATH, "transport_processed.csv"), header=True, inferSchema=True)
    traffic_df = spark.read.csv(os.path.join(DATA_PATH, "traffic_processed.csv"), header=True, inferSchema=True)
    energy_df = spark.read.csv(os.path.join(DATA_PATH, "energy_processed.csv"), header=True, inferSchema=True)
    print("CSV files loaded successfully.")
except Exception as e:
    print(f"CRITICAL ERROR: FAILED TO READ CSV FILE(S)", file=sys.stderr)
    print(f"Error details: {e}", file=sys.stderr)
    print(f"Please verify file content, schema, and file path: {DATA_PATH}", file=sys.stderr)
    spark.stop()
    sys.exit(1)

# Carga la Data Base en la capa analítica de Postgres
load_to_postgres(transport_df, "analytics", "transport", mode="overwrite")
load_to_postgres(traffic_df, "analytics", "traffic", mode="overwrite")
load_to_postgres(energy_df, "analytics", "energy", mode="overwrite")

# Cálculo de Métricas (KPIs)
print("\nStarting KPI calculation with Spark...")

transport_metrics = transport_df.groupBy("route_id").agg(
    round(avg(col("calculated_delay")), 2).alias("avg_delay_minutes"),
    round((sum(when(col("punctuality_status").isin("Early", "On Time"), 1).otherwise(0)) / count("*")) * 100, 2).alias("punctuality_rate_pct"),
    count("*").alias("total_trips"),
    round(max(col("calculated_delay")), 2).alias("max_delay_minutes"),
    sum(when(col("punctuality_status") == "Very Late", 1).otherwise(0)).alias("very_late_count")
)
load_to_postgres(transport_metrics, "analytics", "route_summary", mode="overwrite")

traffic_metrics = traffic_df \
    .withColumn("measurement_hour", hour(col("timestamp"))) \
    .groupBy("zone_id", "measurement_hour").agg(
        round(avg(col("avg_speed")), 2).alias("avg_speed_kmh"),
        round(avg(col("vehicle_count")), 2).alias("avg_vehicle_count"),
        count("*").alias("total_measurements"),
        round((sum(when(col("congestion_level") == "High Congestion", 1).otherwise(0)) / count("*")) * 100, 2).alias("high_congestion_pct")
)
load_to_postgres(traffic_metrics, "analytics", "traffic_summary", mode="overwrite")

energy_metrics = energy_df.groupBy("fuel_type").agg(
    round(avg(col("consumption_rate")), 3).alias("avg_consumption_rate"),
    round(sum(col("estimated_cost")), 2).alias("total_estimated_cost"),
    round(sum(col("distance_km")), 2).alias("total_distance_km")
)
load_to_postgres(energy_metrics, "analytics", "energy_summary", mode="overwrite")

print("\nSpark Pipeline Complete. All base data and summary tables loaded to PostgreSQL.")
spark.stop()
