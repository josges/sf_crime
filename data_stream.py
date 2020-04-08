import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as func


schema = StructType(
    [
        StructField("crime_id", StringType()),
        StructField("original_crime_type_name", StringType()),
        StructField("report_date", TimestampType()),
        StructField("call_date", TimestampType()),
        StructField("offense_date", TimestampType()),
        StructField("call_time", StringType()),
        StructField("call_date_time", TimestampType()),
        StructField("disposition", StringType()),
        StructField("address", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("agency_id", StringType()),
        StructField("address_type", StringType()),
        StructField("common_location", StringType()),
    ]
)


def run_spark_job(spark):
    df = (
        spark.readStream.format("kafka")
        .option("subscribe", "org.sf.police.calls")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200)
        .option("kafka.bootstrap.servers", "localhost:9092")
        .load()
    )
    # Show schema for the incoming resources for checks
    # df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df.select(
        func.from_json(func.col("value"), schema).alias("DF")
    ).select("DF.*")

    # select original_crime_type_name and disposition
    distinct_table = service_table.select("original_crime_type_name", "disposition")

    # count the number of original crime type
    agg_df = distinct_table.groupBy("disposition").agg(
        func.count("original_crime_type_name").alias("original_crime_types")
    )

    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    # write output stream
    query = (
        agg_df.writeStream
        .trigger(processingTime="30 second")
        .outputMode("Complete")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    # get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(
        radio_code_json_filepath
    )

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join on disposition column
    join_query = (
        agg_df.join(radio_code_df, on="disposition")
        .writeStream.format("console")
        .trigger(processingTime="30 second")
        .outputMode("Complete")
        .option("truncate", "false")
        .start()
    )
    # attach a ProgressReporter
    join_query.awaitTermination()
    query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    conf = (
        SparkConf()
        .setMaster("local[*]")
        .set(key="spark.sql.shuffle.partitions", value="1")
        .set(key="spark.default.parallelism", value="1")
        .set(key="spark.executor.memory", value="4g")
        .set(key="spark.driver.memory", value="4g")
        .set(key="spark.streaming.receiver.minRatePerPartition", value="400")
    )
    spark = (
        SparkSession.builder.master("local[*]")
        .config(conf=conf)
        .appName("KafkaSparkStructuredStreaming")
        .getOrCreate()
    )

    logger.info("Spark started")
    # spark.sparkContext.setLogLevel("WARN")

    run_spark_job(spark)

    spark.stop()
