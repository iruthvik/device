import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

def calculateDelta(df: DataFrame, columns: List[String], windowDuration: String, watermarkDuaration: String): DataFrame = {
  val maxColumnsExpr = columns.map(colName => expr(s"max($colName) as max_$colName"))
  val minColumnsExpr = columns.map(colName => expr(s"min($colName) as min_$colName"))
  val allColumnsExpr = maxColumnsExpr ++ minColumnsExpr

  val windowedDF = df.withWatermark("timestamp", watermarkDuaration)
    .groupBy(
      window(col("timestamp"), windowDuration).alias("period"),
      col("devicename"),
      col("interface"),
      row_number().over(Window.partitionBy(col("devicename"), col("interface"), window(col("timestamp"), windowDuration).alias("period")).orderBy(col("timestamp"))).alias("row_num")
    )
    .agg(allColumnsExpr.head, allColumnsExpr.tail: _*)

  val deltaColumns = columns :+ "period" // including for delta time

  val deltaDF = deltaColumns.foldLeft(windowedDF) { (df, colName) =>
    val deltaColumn = when(col("row_num") === 5, col(s"max_$colName") - col(s"min_$colName")).otherwise(null)
    if (colName == "period") {
      df.withColumn("delta_time", expr("UNIX_TIMESTAMP(period.end) - UNIX_TIMESTAMP(period.start)"))
    } else {
      df.withColumn(s"delta_$colName", deltaColumn)
    }
  }.filter(col("row_num") === 5)

  deltaDF
}

------------------------------
\
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

def calculateDelta(df: DataFrame, columns: List[String], windowDuration: String, watermarkDuaration: String): DataFrame = {
  val maxColumnsExpr = columns.map(colName => expr(s"max($colName) as max_$colName"))
  val minColumnsExpr = columns.map(colName => expr(s"min($colName) as min_$colName"))
  val allColumnsExpr = maxColumnsExpr ++ minColumnsExpr

  val windowedDF = df.withWatermark("timestamp", watermarkDuaration)
    .withColumn("row_num", row_number().over(
      Window.partitionBy(col("devicename"), col("interface"))
        .orderBy(col("timestamp"))
    ))
    .groupBy(
      window(col("timestamp"), windowDuration).alias("period"),
      col("devicename"),
      col("interface"),
      col("row_num")
    )
    .agg(allColumnsExpr.head, allColumnsExpr.tail: _*)

  val deltaColumns = columns :+ "period" // including for delta time

  val deltaDF = deltaColumns.foldLeft(windowedDF) { (df, colName) =>
    val deltaColumn = when(col("row_num") === 5, col(s"max_$colName") - col(s"min_$colName")).otherwise(null)
    if (colName == "period") {
      df.withColumn("delta_time", expr("UNIX_TIMESTAMP(period.end) - UNIX_TIMESTAMP(period.start)"))
    } else {
      df.withColumn(s"delta_$colName", deltaColumn)
    }
  }.filter(col("row_num") === 5)

  deltaDF
}

------------------------------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

def calculateDelta(df: DataFrame, columns: List[String], windowDuration: String, watermarkDuaration: String): DataFrame = {
  val maxColumnsExpr = columns.map(colName => expr(s"max($colName) as max_$colName"))
  val minColumnsExpr = columns.map(colName => expr(s"min($colName) as min_$colName"))
  val allColumnsExpr = maxColumnsExpr ++ minColumnsExpr

  val windowedDF = df.withWatermark("timestamp", watermarkDuaration)
    .groupBy(
      window(col("timestamp"), windowDuration).alias("period"),
      col("devicename"),
      col("interface")
    )
    .agg(
      allColumnsExpr.head,
      allColumnsExpr.tail: _*,
      row_number().over(
        Window.partitionBy(col("devicename"), col("interface"), window(col("timestamp"), windowDuration))
          .orderBy(col("timestamp"))
      ).alias("row_num")
    )

  val deltaColumns = columns :+ "period" // including for delta time

  val deltaDF = deltaColumns.foldLeft(windowedDF) { (df, colName) =>
    val deltaColumn = when(col("row_num") === 5, col(s"max_$colName") - col(s"min_$colName")).otherwise(null)
    if (colName == "period") {
      df.withColumn("delta_time_ms", expr("UNIX_TIMESTAMP(period.end) - UNIX_TIMESTAMP(period.start)"))
    } else {
      df.withColumn(s"delta_$colName", deltaColumn)
    }
  }.filter(col("row_num") === 5)

  deltaDF
}



-----------------
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

def calculateDelta(df: DataFrame, columns: List[String], windowDuration: String, watermarkDuaration: String): DataFrame = {
  val maxColumnsExpr = columns.map(colName => expr(s"max($colName) as max_$colName"))
  val minColumnsExpr = columns.map(colName => expr(s"min($colName) as min_$colName"))
  val allColumnsExpr = maxColumnsExpr ++ minColumnsExpr

  val windowedDF = df.withWatermark("timestamp", watermarkDuaration)
    .groupBy(
      window(col("timestamp"), windowDuration).alias("period"),
      col("devicename"),
      col("interface")
    )
    .agg(
      allColumnsExpr.head,
      allColumnsExpr.tail: _*,
      row_number().over(
        Window.partitionBy(col("devicename"), col("interface"), window(col("timestamp"), windowDuration))
          .orderBy(col("timestamp"))
      ).alias("row_num")
    )

  val deltaColumns = columns :+ "period" // including for delta time

  val deltaDF = deltaColumns.foldLeft(windowedDF) { (df, colName) =>
    val deltaColumn = when(col("row_num") === 5, col(s"max_$colName") - col(s"min_$colName")).otherwise(null)
    if (colName == "period") {
      df.withColumn("delta_time_ms", expr("UNIX_TIMESTAMP(period.end) - UNIX_TIMESTAMP(period.start)"))
    } else {
      df.withColumn(s"delta_$colName", deltaColumn)
    }
  }.filter(col("row_num") === 5)

  deltaDF
}

----------------------

--------------

import org.apache.spark.sql.functions._

def calculateDelta(df: org.apache.spark.sql.DataFrame, columns: List[String], windowDuration: String, watermarkDuaration: String): org.apache.spark.sql.DataFrame = {
  val maxColumnsExpr = columns.map(colName => expr(s"max($colName) as max_$colName"))
  val minColumnsExpr = columns.map(colName => expr(s"min($colName) as min_$colName"))
  val allColumnsExper = maxColumnsExpr ++ minColumnsExpr

  val aggregatedDF = df
    .withWatermark("timestamp", watermarkDuaration)
    .groupBy(
      window(col("timestamp"), windowDuration, "5 minutes"), // Modified window definition
      col("devicename"),
      col("interface")
    )
    .agg(allColumnsExper.head, allColumnsExper.tail: _*)

  val deltaColumns = columns :+ "window" // including for delta time

  val deltaDF = deltaColumns.foldLeft(aggregatedDF) { (df, colName) =>
    val deltaColumn = col(s"max_$colName") - col(s"min_$colName")
    if (colName == "window") {
      df.withColumn("delta_time", expr("UNIX_TIMESTAMP(window.end) - UNIX_TIMESTAMP(window.start)"))
    } else {
      df.withColumn(s"delta_$colName", deltaColumn)
    }
  }
  deltaDF.drop("window") // Drop the 'window' column if you don't need it
}

val deltaDF = calculateDelta(finalDF, List("in_octets","out_octets","in_packets","out_packets"), "5 minutes", "5 minutes")
deltaDF.show()

