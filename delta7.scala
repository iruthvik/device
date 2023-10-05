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
