# Program to process logs in data-generator/random.csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col, sum, count, to_date, count_distinct

#Initialization of DataFrame
spark =  SparkSession.builder.appName("Process Logs").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark\
    .read\
    .csv("file:///home/hadoop/ecommerce-data-pipeline/data-generator/random.csv"\
         , header = True\
            , inferSchema = True)

#Cleaning Up Columns
df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("price", col("price").cast("double"))
df_date = df.withColumn("date", to_date("timestamp"))
df_date = df_date.withColumnRenamed("date", "event_date")

#Event Based Logs
df.groupBy("type_of_event")\
    .agg(count("*").alias("count_of_event")).\
    coalesce(1)\
    .write\
    .option("header",True)\
    .option("delimiter", ",")\
        .mode("overwrite")\
            .csv("file:///home/hadoop/ecommerce-data-pipeline/output/event_count")

#Product Based Logs
product_count = df.groupBy("product_id")\
    .agg(sum("price").alias("product_revenue"),\
         count("*").alias("product_event_count"))

product_events = df.groupBy("product_id")\
    .pivot("type_of_event")\
    .agg(count("*"))\
    .fillna(0)\
    .orderBy("product_id")

join_product = product_count.join(product_events, on="product_id", how="left").orderBy("product_id")

join_product.coalesce(1)\
    .write\
    .option("header",True)\
    .option("delimiter", ",")\
        .mode("overwrite")\
            .csv("file:///home/hadoop/ecommerce-data-pipeline/output/product_count")
 
#Date Based Logs
event_counts = df_date.groupBy("event_date")\
    .pivot("type_of_event")\
    .agg(count("*"))\
    .fillna(0)\
    .orderBy("event_date")

total_revenue = df_date.groupBy("event_date")\
    .agg(sum("price").alias("total_revenue"))\
    .orderBy("event_date")

join_events = event_counts.join(total_revenue, on="event_date", how="left").orderBy("event_date")

join_events.coalesce(1)\
    .write\
    .option("header",True)\
    .option("delimiter", ",")\
        .mode("overwrite")\
            .csv("file:///home/hadoop/ecommerce-data-pipeline/output/daily_events")