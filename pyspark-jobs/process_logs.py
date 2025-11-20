# Program to process logs in data-generator/random.csv
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, count, hour, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType

yesterday = date.today() - timedelta(days=1)

#Initialization of DataFrame
log_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("type_of_event", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_revenue", DecimalType(12,2), True),
    StructField("product_price", DecimalType(5,2), True)
])

spark =  SparkSession.builder.appName("Process Logs").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark\
    .read\
    .csv(f"file:///home/hadoop/ecommerce-data-pipeline/logs/log_{yesterday}.csv"\
        , header = True\
        , schema = log_schema)

df_time = hour(col("event_timestamp")).alias("event_time")
df_dt = df.withColumn("event_date", to_date(col("event_timestamp")))

#Query to Allow for SQL queries in PySpark
df_dt.createOrReplaceTempView("log_data")

#Event Based Logs
df_dt.groupBy("event_date","type_of_event")\
    .agg(
        count("*").alias("count_of_event"),
        sum("product_revenue").alias("event_revenue"))\
    .orderBy(col("event_date").desc(), col("event_revenue").desc())\
    .write\
    .mode("overwrite")\
    .parquet("file:///home/hadoop/ecommerce-data-pipeline/output/event_count")

#Product Based Logs
product_count = spark.sql("""
                        SELECT
                            product_id,
                            event_date,
                            COUNT(*) AS product_event_count,
                            SUM(product_revenue) AS total_product_revenue,
                            MAX(product_revenue) AS product_price
                        FROM log_data
                        GROUP BY product_id, event_date
                        ORDER BY product_id, event_date; 
                """)


product_events = df_dt.groupBy("product_id")\
    .pivot("type_of_event")\
    .agg(count("*"))\
    .fillna(0)\
    .orderBy("product_id")

join_product = product_events.join(product_count, on="product_id", how="left").orderBy("product_id")

join_product\
    .write\
    .mode("overwrite")\
    .parquet("file:///home/hadoop/ecommerce-data-pipeline/output/product_count")
 
#Time Based Table
df_select = df.select(
    when(hour(col("event_timestamp")) < 6, '00:00 to 05:59')
    .when(hour(col("event_timestamp")) < 12, '06:00 to 11:59')
    .when(hour(col("event_timestamp")) < 18, '12:00 to 17:59')
    .otherwise('18:00 to 23:59')
    .alias("time_interval"),
    col("product_revenue"),
    col("type_of_event")
)

event_counts = df_select.groupBy("time_interval")\
    .pivot("type_of_event")\
    .agg(count("*"))\
    .fillna(0)\
    .orderBy("time_interval")

total_revenue = spark.sql("""
                          WITH time_interval AS
                          (
                          SELECT 
                          CASE 
                            WHEN HOUR(event_timestamp) < 6 THEN '00:00 to 05:59'
                            WHEN HOUR(event_timestamp) < 12 THEN '06:00 to 11:59'
                            WHEN HOUR(event_timestamp) < 18 THEN '12:00 to 17:59'
                            ELSE '18:00 to 23:59'
                          END AS time_interval,
                          product_revenue,
                          event_date,
                          type_of_event
                          FROM log_data
                          )
                          SELECT
                            time_interval,
                            event_date,
                            COUNT(*) as total_event_count,
                            SUM(product_revenue) AS revenue_by_time_interval
                          FROM time_interval
                          GROUP BY time_interval, event_date
                          ORDER BY time_interval;
                          """)

join_events = event_counts.join(total_revenue, on="time_interval", how="left").orderBy("time_interval")

join_events\
    .write\
    .mode("overwrite")\
    .parquet("file:///home/hadoop/ecommerce-data-pipeline/output/time_events")