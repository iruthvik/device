import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, col}

// Assuming you have a timestamp column named 'timestamp' and a bytes column named 'bytes' in your DataFrame 'df'

// Create a window specification partitioned by 5-minute intervals
val windowSpec = Window.orderBy("timestamp").rangeBetween(-5 * 60, 0)

// Calculate the lag of bytes and timestamp
val lagBytes = lag(col("bytes"), 4).over(windowSpec)
val lagTimestamp = lag(col("timestamp"), 4).over(windowSpec)

// Calculate the interval in seconds
val intervalSeconds = (col("timestamp").cast("long") - lagTimestamp.cast("long"))

// Calculate the difference between 5th record's bytes and 1st record's bytes
val bytesDifference = col("bytes") - lagBytes

// Select the required columns
val resultDF = df
  .select(col("timestamp"), bytesDifference.alias("BytesDifference"), intervalSeconds.alias("IntervalSeconds"))
  .filter(intervalSeconds.isNotNull)

// Show or perform further operations on resultDF as needed
resultDF.show()
