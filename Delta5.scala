import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first, last, unix_timestamp}

// Assuming you have a DataFrame named 'df' with 'timestamp' and 'bytes' columns

// Create a window specification partitioned by 5-minute intervals
val windowSpec = Window.orderBy("timestamp").rangeBetween(-4 * 60, 0)

// Calculate the sum of bytes for the 5th record and 1st record in the window
val fifthBytes = last(col("bytes")).over(windowSpec)
val firstBytes = first(col("bytes")).over(windowSpec)

// Calculate the maximum timestamp and minimum timestamp in the window
val maxTimestamp = last(unix_timestamp(col("timestamp"))).over(windowSpec)
val minTimestamp = first(unix_timestamp(col("timestamp"))).over(windowSpec)

// Calculate the interval in seconds
val intervalSeconds = maxTimestamp - minTimestamp

// Calculate the difference between 5th record's bytes and 1st record's bytes
val bytesDifference = fifthBytes - firstBytes

// Select the required columns
val resultDF = df
  .select(col("timestamp"), bytesDifference.alias("BytesDifference"), intervalSeconds.alias("IntervalSeconds"))
  .filter(intervalSeconds.isNotNull)

// Show or perform further operations on resultDF as needed
resultDF.show()



import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum, unix_timestamp}

// Assuming you have a DataFrame named 'df' with 'timestamp' and 'bytes' columns

// Create a window specification partitioned by 5-minute intervals
val windowSpec = Window.orderBy(unix_timestamp(col("timestamp"))).rangeBetween(-240, 0)

// Calculate the sum of bytes for the 5th record and 1st record in the window
val fifthBytes = sum(col("bytes")).over(windowSpec)
val firstBytes = sum(col("bytes")).over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

// Calculate the maximum timestamp and minimum timestamp in the window
val maxTimestamp = max(unix_timestamp(col("timestamp"))).over(windowSpec)
val minTimestamp = min(unix_timestamp(col("timestamp"))).over(windowSpec)

// Calculate the interval in seconds
val intervalSeconds = maxTimestamp - minTimestamp

// Calculate the difference between 5th record's bytes and 1st record's bytes
val bytesDifference = fifthBytes - firstBytes

// Select the required columns
val resultDF = df
  .select(col("timestamp"), bytesDifference.alias("BytesDifference"), intervalSeconds.alias("IntervalSeconds"))
  .filter(intervalSeconds.isNotNull)

// Show or perform further operations on resultDF as needed
resultDF.show()
....




import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum, unix_timestamp}
import org.apache.spark.sql.DataFrame

// Assuming you have a DataFrame named 'df' with 'timestamp' and 'bytes' columns

// Create a window specification partitioned by 5-minute intervals
val windowSpec = Window.orderBy(unix_timestamp(col("timestamp"))).rangeBetween(-240, 0)

// Calculate the sum of bytes for the 5th record and 1st record in the window
val fifthBytes = sum(col("bytes")).over(windowSpec)
val firstBytes = sum(col("bytes")).over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

// Calculate the maximum timestamp and minimum timestamp in the window
val maxTimestamp = max(unix_timestamp(col("timestamp"))).over(windowSpec)
val minTimestamp = min(unix_timestamp(col("timestamp"))).over(windowSpec)

// Calculate the interval in seconds
val intervalSeconds = maxTimestamp - minTimestamp

// Create a Common Table Expression (CTE)
df.createOrReplaceTempView("temp")

val query = s"""
  SELECT timestamp, BytesDifference, IntervalSeconds
  FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY timestamp) as rn
    FROM temp
  ) temp
  WHERE rn % 5 = 0
"""

// Execute the query and store the result in a DataFrame
val resultDF: DataFrame = spark.sql(query)

// Show or perform further operations on resultDF as needed
resultDF.show()
