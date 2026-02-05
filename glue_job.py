import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    year, month, col, avg, count, when,
    to_timestamp, coalesce, lit
)

# ==============================
# JOB INIT
# ==============================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ==============================
# READ RAW DATA
# ==============================
raw_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://airport-airline-operations-analytics-platform/raw/"],
        "recurse": True
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ",",
        "quoteChar": '"'
    }
).toDF()

# ==============================
# CLEAN + DERIVED COLUMNS
# ==============================
df = (
    raw_df
    .withColumn("FL_DATE", to_timestamp(col("FL_DATE")))
    .withColumn("year", year("FL_DATE"))
    .withColumn("month", month("FL_DATE"))
    .withColumn(
        "on_time_dep",
        when(col("DEP_DELAY").isNotNull() & (col("DEP_DELAY") <= 15), 1).otherwise(0)
    )
    .withColumn(
        "on_time_arr",
        when(col("ARR_DELAY").isNotNull() & (col("ARR_DELAY") <= 15), 1).otherwise(0)
    )
)

# ==============================
# BASE AGGREGATION
# ==============================
base_agg = (
    df.groupBy("year","month","ORIGIN","DEST","OP_CARRIER")
    .agg(
        count("*").alias("total_flights"),
        avg("DEP_DELAY").alias("avg_dep_delay"),
        avg("ARR_DELAY").alias("avg_arr_delay"),
        avg("TAXI_OUT").alias("avg_taxi_out"),
        avg("TAXI_IN").alias("avg_taxi_in"),
        avg("AIR_TIME").alias("avg_air_time"),
        (avg("on_time_dep") * 100).alias("on_time_dep_pct"),
        (avg("on_time_arr") * 100).alias("on_time_arr_pct"),
        avg("O_PRCP").alias("avg_o_prcp"),
        avg("O_WSPD").alias("avg_o_wspd"),
        avg("D_PRCP").alias("avg_d_prcp"),
        avg("D_WSPD").alias("avg_d_wspd")
    )
)

# ==============================
# LOOKUP TABLES
# ==============================
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

# ==============================
# CUSTOMERS GOLD (WITH LOOKUPS)
# ==============================
customers_gold = (
    base_agg
    .join(origin_lkp, "ORIGIN", "left")
    .join(dest_lkp, "DEST", "left")
    .join(carrier_lkp, "OP_CARRIER", "left")
)

customers_gold.write \
    .mode("overwrite") \
    .parquet("s3://airport-airline-operations-analytics-platform/gold/customers/")

# ==============================
# AIRLINE GOLD KPIs
# ==============================
airline_gold = (
    customers_gold

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

airline_gold.write \
    .mode("overwrite") \
    .parquet("s3://airport-airline-operations-analytics-platform/gold/airline/")

# ==============================
# COMMIT
# ==============================
job.commit()
