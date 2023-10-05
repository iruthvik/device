import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Create a SparkSession
val spark = SparkSession.builder()
  .appName("Delta Calculation")
  .getOrCreate()

// Define a schema for the DataFrame
val schema = StructType(Seq(
  StructField("Devicename", StringType, true),
  StructField("inteface", StringType, true),
  StructField("timestamp", TimestampType, true),
  StructField("in_octets", IntegerType, true),
  StructField("out_octets", IntegerType, true)
))

// Read data from Pulsar using Structured Streaming
val pulsarStream = spark
  .readStream
  .format("pulsar")
  .option("service.url", "pulsar://<pulsar-broker-url>")
  .option("admin.url", "http://<pulsar-admin-url>")
  .option("topic", "ngcp")
  .schema(schema)
  .load()

// Define the window duration
val windowDuration = "5 minutes"

// Calculate the start and end of the window for each record based on timestamp
val windowedStream = pulsarStream
  .withColumn("window_start", 
    expr(s"timestamp - INTERVAL CAST(FLOOR(UNIX_TIMESTAMP(timestamp) / (300)) * (300) AS INT) SECONDS"))
  .withColumn("window_end", 
    expr(s"timestamp + INTERVAL 299 SECONDS"))
  .groupBy(
    window(col("window_start"), windowDuration).alias("window"),
    col("Devicename"),
    col("inteface")
  )
  .agg(
    max("in_octets").alias("max_in_octets"),
    min("in_octets").alias("min_in_octets"),
    max("out_octets").alias("max_out_octets"),
    min("out_octets").alias("min_out_octets")
  )

// Calculate delta for each 5-minute window
val deltaStream = windowedStream
  .withColumn("delta_in_octets", col("max_in_octets") - col("min_in_octets"))
  .withColumn("delta_out_octets", col("max_out_octets") - col("min_out_octets"))

// Output the results to a sink (e.g., console, Parquet, etc.)
val query = deltaStream
  .writeStream
  .outputMode("append")
  .format("console")
  .start()

query.awaitTermination()
