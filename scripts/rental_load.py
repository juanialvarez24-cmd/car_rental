# -*- coding: utf-8 -*-
# Usage: chmod +x rental_load.py && spark-submit rental_load.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, lit, lower, trim, upper, when
from pyspark.sql.types import IntegerType

# ===============================================
# 0. SPARK INITIALIZATION
# ===============================================

try:
    spark = SparkSession.builder \
        .appName("RentalIngestLoad") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .enableHiveSupport() \
        .getOrCreate()
    print("Spark Session initialized successfully.")

except Exception as e:
    print(f"ERROR initializing Spark Session: {e}")
    exit(1)

HDFS_LANDING_PATH = "hdfs://172.17.0.2:9000/home/hadoop/landing/rental"

# ===============================================
# 1. MAPPING
# ===============================================
RENTAL_MAPEO = [
    ("fuelType", "fuelType"),
    ("rating", "rating"),
    ("renterTripsTaken", "renterTripsTaken"),
    ("reviewCount", "reviewCount"),
    ("location.city", "city"),
    ("location.state", "state_id"),
    ("owner.id", "owner_id"),
    ("rate.daily", "rate_daily"),
    ("vehicle.make", "make"),
    ("vehicle.model", "model"),
    ("vehicle.year", "year")
]

GEO_MAPEO = [
    ("United States Postal Service state abbreviation", "state_id_geo"),
    ("Official Name State", "state_name"),
]

# ===============================================
# 2. RENTAL DATA PROCESSING
# ===============================================

try:
    print("\n--- 2. Processing data ---")
  
    path_rental = f"{HDFS_LANDING_PATH}/rental_data.csv"
    path_geo = f"{HDFS_LANDING_PATH}/geo_ref.csv"
  
    rental_df = spark.read.csv(path_rental, header=True, inferSchema=True, sep=',')
    geo_df = spark.read.csv(path_geo, header=True, inferSchema=True, sep=';')

    selection_expressions = [col(f"`{original}`").alias(final)
                             for original, final in RENTAL_MAPEO]

    rental_seleccionado = rental_df.select(*selection_expressions)
  
    geo_seleccionado = geo_df.select(
        *[col(csv_name).alias(hive_name) for csv_name, hive_name in GEO_MAPEO]
    )

    rental_seleccionado = rental_seleccionado.withColumn("state_id", upper(trim(col("state_id").cast("string"))))
    geo_seleccionado = geo_seleccionado.withColumn("state_id_geo", upper(trim(col("state_id_geo").cast("string"))))

    df_union = rental_seleccionado.join(
        geo_seleccionado,
        col("state_id") == col("state_id_geo"),
        "left_outer"
    )

    df_filtrado_rating = df_union.filter(
        (col("rating").isNotNull()) & (trim(col("rating")) != lit(""))
    )

    df_filtrado_texas = df_filtrado_rating.filter(
        (col("state_id_geo").isNull()) | (lower(trim(col("state_id_geo"))) != lit("tx"))
    )

    df_filtrado = df_filtrado_texas.drop("state_id", "state_id_geo")
    print(f"Data processed successfully: {df_filtrado.count()}")

except Exception as e:
    print(f"ERROR processing rental data: {e}")
    spark.stop()
    exit(1)

# ===============================================
# 3. FINAL FILTERING AND STRICT SELECTION WITH CAST
# ===============================================
try:
    print("\n--- 3. Performing filtering and selecting final columns with CASTING ---")

    fuelType_limpio = when(
        (col("fuelType").isNull()) | (trim(col("fuelType")) == lit("")),
        lit("unknown")
    ).otherwise(lower(col("fuelType")))

    df_final = df_filtrado.select(
        fuelType_limpio.cast("string").alias("fuelType"),
        round(col("rating")).cast(IntegerType()).alias("rating"),
        col("renterTripsTaken").cast(IntegerType()).alias("renterTripsTaken"),
        col("reviewCount").cast(IntegerType()).alias("reviewCount"),
        col("city").cast("string").alias("city"),
        col("state_name").cast("string").alias("state_name),
        col("owner_id").cast(IntegerType()).alias("owner_id"),
        col("rate_daily").cast(IntegerType()).alias("rate_daily"),
        col("make").cast("string").alias("make"),
        col("model").cast("string").alias("model"),
        col("year").cast(IntegerType()).alias("year")
    )

    print(f"Final records (11 columns): {df_final.count()}")

except Exception as e:
    print(f"ERROR filtering and selecting data: {e}")
    spark.stop()
    exit(1)
# ===============================================
# 4. HIVE INSERTION
# ===============================================
TABLA_HIVE="car_rental_db.car_rental_analytics"
try:
    print(f"\n--- 4. Inserting final result into Hive: {TABLA_HIVE} ---")

    df_final.write.mode("overwrite").insertInto(TABLA_HIVE)

    print("Hive load completed successfully.")

except Exception as e:
    print(f"ERROR inserting into Hive: {e}")

spark.stop()
print("\nPySpark ETL process finished.")
