import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col, when, hour, concat_ws,
    sum as _sum, count as _count,
    year, month, avg, to_timestamp,
    coalesce, lit
)

# ==================================================
# 1. JOB INIT
# ==================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.shuffle.partitions", "200")

# ==================================================
# 2. READ RAW DATA (ONCE)
# ==================================================
raw_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("s3://airport-airline-operations-analytics-platform/raw/*.csv")

# ==================================================
# 3. COMMON CLEANING
# ==================================================
df = (
    raw_df
    .withColumn("FL_DATE", to_timestamp(col("FL_DATE")))
    .withColumn(
        "clean_arr_delay",
        when(col("ARR_DELAY").isNull() | (col("ARR_DELAY") < 0), 0)
        .otherwise(col("ARR_DELAY"))
    )
    .withColumn(
        "clean_dep_delay",
        when(col("DEP_DELAY").isNull(), 0).otherwise(col("DEP_DELAY"))
    )
    .withColumn(
        "taxi_out_clean",
        when(col("TAXI_OUT").isNull(), 0).otherwise(col("TAXI_OUT"))
    )
    .withColumn(
        "taxi_in_clean",
        when(col("TAXI_IN").isNull(), 0).otherwise(col("TAXI_IN"))
    )
)

# ==================================================
# 4. LOOKUP TABLES (EXACT S3 PATHS)
# ==================================================
origin_lkp = spark.read.option("header", True) \
    .csv("s3://airport-airline-operations-analytics-platform/reference/ORIGIN .csv") \
    .select(
        col("Code").alias("ORIGIN"),
        col("Airport Name").alias("origin_airport_name")
    )

dest_lkp = spark.read.option("header", True) \
    .csv("s3://airport-airline-operations-analytics-platform/reference/DEST Airport.csv") \
    .select(
        col("Code").alias("DEST"),
        col("Airport Name").alias("dest_airport_name")
    )

carrier_lkp = spark.read.option("header", True) \
    .csv("s3://airport-airline-operations-analytics-platform/reference/OP_Carrier code to full airline name.csv") \
    .select(
        col("Code").alias("OP_CARRIER"),
        col("Airline Name").alias("carrier_full_name")
    )

# ==================================================
# ================= PIPELINE 1 =====================
# SILVER – AIRLINE (ROUTE + HOUR)
# ==================================================
df_airline = (
    df
    .withColumn("dep_hour", hour(col("CRS_DEP_TIME")))
    .withColumn("is_morning_peak", when((col("dep_hour") >= 6) & (col("dep_hour") <= 9), 1).otherwise(0))
    .withColumn("is_evening_peak", when((col("dep_hour") >= 17) & (col("dep_hour") <= 21), 1).otherwise(0))
    .withColumn("ground_delay", col("taxi_out_clean") + col("clean_dep_delay"))
    .withColumn("arrival_congestion", col("taxi_in_clean"))
    .withColumn(
        "air_delay",
        when(col("AIR_TIME").isNull(), 0)
        .otherwise(col("AIR_TIME") - col("CRS_ELAPSED_TIME"))
    )
    .withColumn("route", concat_ws(" → ", col("ORIGIN"), col("DEST")))
)

airline_silver = (
    df_airline
    .groupBy("OP_CARRIER", "ORIGIN", "DEST", "route", "dep_hour")
    .agg(
        _count("*").alias("total_flights"),
        _sum("clean_dep_delay").alias("total_dep_delay"),
        _sum("clean_arr_delay").alias("total_arr_delay"),
        _sum("ground_delay").alias("total_ground_delay"),
        _sum("air_delay").alias("total_air_delay"),
        _sum("arrival_congestion").alias("total_arrival_congestion"),
        _sum("taxi_out_clean").alias("total_taxi_out"),
        _sum("taxi_in_clean").alias("total_taxi_in"),
        _sum("is_morning_peak").alias("morning_peak_flights"),
        _sum("is_evening_peak").alias("evening_peak_flights")
    )
    .withColumn(
        "congestion_score",
        (col("total_ground_delay") + col("total_arrival_congestion")) / col("total_flights")
    )
    .withColumn(
        "efficiency_score",
        1 / (1 + col("congestion_score"))
    )
    .join(origin_lkp, "ORIGIN", "left")
    .join(dest_lkp, "DEST", "left")
    .join(carrier_lkp, "OP_CARRIER", "left")
)

airline_silver.write \
    .mode("overwrite") \
    .parquet("s3://airport-airline-operations-analytics-platform/silver/airline/")

# ==================================================
# ================= PIPELINE 2 =====================
# SILVER – CUSTOMERS (MONTHLY)
# ==================================================
df_customers = (
    df
    .withColumn("year", year("FL_DATE"))
    .withColumn("month", month("FL_DATE"))
    .withColumn("on_time_dep", when(col("DEP_DELAY") <= 15, 1).otherwise(0))
    .withColumn("on_time_arr", when(col("ARR_DELAY") <= 15, 1).otherwise(0))
)

customers_silver = (
    df_customers
    .groupBy("year", "month", "ORIGIN", "DEST", "OP_CARRIER")
    .agg(
        _count("*").alias("total_flights"),
        avg("DEP_DELAY").alias("avg_dep_delay"),
        avg("ARR_DELAY").alias("avg_arr_delay"),
        (avg("on_time_dep") * 100).alias("on_time_dep_pct"),
        (avg("on_time_arr") * 100).alias("on_time_arr_pct"),
        avg("TAXI_OUT").alias("avg_taxi_out"),
        avg("TAXI_IN").alias("avg_taxi_in"),
        avg("AIR_TIME").alias("avg_air_time"),
        avg("O_PRCP").alias("avg_o_prcp"),
        avg("O_WSPD").alias("avg_o_wspd"),
        avg("D_PRCP").alias("avg_d_prcp"),
        avg("D_WSPD").alias("avg_d_wspd")
    )
    .join(origin_lkp, "ORIGIN", "left")
    .join(dest_lkp, "DEST", "left")
    .join(carrier_lkp, "OP_CARRIER", "left")
)

customers_silver.write \
    .mode("overwrite") \
    .parquet("s3://airport-airline-operations-analytics-platform/silver/customers/")

# ==================================================
# ================= PIPELINE 3 =====================
# GOLD – OPERATIONAL HEALTH
# ==================================================
gold_df = (
    customers_silver
    .withColumn(
        "journey_friction_index",
        coalesce(col("avg_dep_delay"), lit(0)) +
        coalesce(col("avg_arr_delay"), lit(0)) +
        coalesce(col("avg_taxi_out"), lit(0)) +
        coalesce(col("avg_taxi_in"), lit(0))
    )
    .withColumn(
        "airport_bottleneck_score",
        (coalesce(col("avg_taxi_out"), lit(0)) +
         coalesce(col("avg_taxi_in"), lit(0))) /
        coalesce(col("avg_air_time"), lit(1))
    )
    .withColumn(
        "airline_reliability_score",
        (col("on_time_arr_pct") * 0.6) +
        (col("on_time_dep_pct") * 0.4)
    )
    .withColumn(
        "weather_disruption_index",
        coalesce(col("avg_o_prcp"), lit(0)) +
        coalesce(col("avg_o_wspd"), lit(0)) +
        coalesce(col("avg_d_prcp"), lit(0)) +
        coalesce(col("avg_d_wspd"), lit(0))
    )
    .withColumn(
        "operational_health_score",
        lit(100)
        - col("journey_friction_index")
        - (col("airport_bottleneck_score") * 10)
        - (col("weather_disruption_index") * 2)
        + col("airline_reliability_score")
    )
)

gold_df.write \
    .mode("overwrite") \
    .parquet(
        "s3://airport-airline-operations-analytics-platform/silver/airline_airport_operational_health/"
    )

# ==================================================
# JOB COMMIT
# ==================================================
job.commit()
