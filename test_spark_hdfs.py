from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkHDFSTest").getOrCreate()

try:
    path = "hdfs:///user/hadoop/spark_test/spark_input.txt"

    df = spark.read.text(path)

    rows = df.count()
    apples = df.filter(df.value == "apple").count()

    print(f"Total rows: {rows}")
    print(f"Apple rows: {apples}")

    assert rows == 4, f"Expected 4 rows, got {rows}"
    assert apples == 2, f"Expected 2 apples, got {apples}"

    print("Spark HDFS test PASSED")
finally:
    spark.stop()
