from pyspark.sql import SparkSession

# Spark Parameters
SPARK_APP_NAME = "Data Insights Challenge"
SPARK_EAGER_VAL = "spark.sql.repl.eagerEval.enabled"
SPARK_EAGER_VAL_SET = True
SPARK_CACHE_METADATA = "spark.sql.parquet.cacheMetadata"
SPARK_CACHE_METADATA_SET = "true"
SPARK_TIMEZONE = "spark.sql.session.timeZone"
SPARK_TIMEZONE_SET = "Etc/UTC"
SPARK_DRIVER_MEMORY = "spark.driver.memory"
SPARK_DRIVER_MEMORY_SET = "32g"
SPARK_AUTOBROADCAST_THRESHOLD = "spark.sql.autoBroadcastJoinThreshold"
SPARK_AUTOBROADCAST_THRESHOLD_SET = "-1"
SPARK_EXECUTOR_MEM_OVERHEAD = "spark.executor.memoryOverhead"
SPARK_EXECUTOR_MEM_OVERHEAD_SET = "1500"

# Column names
MAKER = "maker"
MODEL = "model"
ZERO = 0


def create_spark() -> SparkSession:
    """
    Create a spark session
    - Parameters:
        - None
    - Returns:
        - SparkSession
    """
    spark = (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config(SPARK_EAGER_VAL, SPARK_EAGER_VAL_SET)
        .config(SPARK_CACHE_METADATA, SPARK_CACHE_METADATA_SET)
        .config(SPARK_TIMEZONE, SPARK_TIMEZONE_SET)
        .config(SPARK_DRIVER_MEMORY, SPARK_DRIVER_MEMORY_SET)
        .config(SPARK_AUTOBROADCAST_THRESHOLD, SPARK_AUTOBROADCAST_THRESHOLD_SET)
        .config(SPARK_EXECUTOR_MEM_OVERHEAD, SPARK_EXECUTOR_MEM_OVERHEAD_SET)
        .getOrCreate()
    )
    return spark
