package esgi.circulation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Clean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val inputFile = args(0)
    val outputFile = args(1)
    val df = spark.read.option("header", "true").option("delimiter", ";").csv(inputFile)

    val res = addDateColumns(df, "t_1h")

    res.write.partitionBy("year", "month", "day").mode(SaveMode.Overwrite).parquet(outputFile)
  }

  def addDateColumns(df: DataFrame, dateCol: String): DataFrame = {
    df
      .withColumn("year", substring(col(dateCol), 0, 4))
      .withColumn("month", substring(col(dateCol), 6, 2))
      .withColumn("day", substring(col(dateCol), 9, 2))
  }
}