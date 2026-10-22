from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = "hdfs:///user/hadoop/hw4/input/apartments.csv"
OUTPUT = "hdfs:///user/hadoop/hw4/apartments_stats"

#сессия spark с hive
spark = (
    SparkSession.builder
    .appName("hw4_apartments")
    .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")
    .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)
print("master:", spark.sparkContext.master)
print("application id:", spark.sparkContext.applicationId)

#читаем csv из hdfs
raw = spark.read.option("header", "true").csv(INPUT)
print("строк в исходных данных:", raw.count())

#приводим типы
df = (
    raw.withColumn("id", F.col("id").cast("int"))
    .withColumn("rooms", F.col("rooms").cast("int"))
    .withColumn("area", F.col("area").cast("double"))
    .withColumn("price", F.col("price").cast("long"))
    .withColumn("sale_date", F.to_date("sale_date"))
)

#убираем битые строки
df = df.filter(F.col("price").isNotNull() & (F.col("area") > 0))
print("строк после очистки:", df.count())

#новые колонки
df = (
    df.withColumn("price_per_m2", F.round(F.col("price") / F.col("area"), 2))
    .withColumn("month", F.date_format("sale_date", "yyyy-MM"))
)

#агрегация
stats = df.groupBy("month", "city", "rooms").agg(
    F.count("*").alias("deals"),
    F.round(F.avg("price")).cast("long").alias("avg_price"),
    F.round(F.avg("price_per_m2"), 2).alias("avg_price_per_m2"),
    F.min("price").alias("min_price"),
    F.max("price").alias("max_price"),
)

#сохраняем таблицу с партициями
spark.sql("CREATE DATABASE IF NOT EXISTS hw4")
(
    stats.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("month")
    .option("path", OUTPUT)
    .saveAsTable("hw4.apartments_stats")
)

spark.sql("SHOW PARTITIONS hw4.apartments_stats").show(truncate=False)
spark.table("hw4.apartments_stats").orderBy("month", "city", "rooms").show(10)

spark.stop()
