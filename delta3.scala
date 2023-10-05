// Calculate the start and end of the window for each record based on timestamp
val windowedStream = pulsarStream
  .withColumn("window_start", 
    expr(s"timestamp - INTERVAL CAST(FLOOR(UNIX_TIMESTAMP(timestamp) / 300) * 300 AS INT) SECONDS"))
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
