package esgi.circulation

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Jointure {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val inputFile = args(0)
    val joinFile = args(1)
    val outputFile = args(2)
    val df = spark.read.parquet(inputFile)
    val joinDf = spark.read.parquet(joinFile)

    val windowIUAC = Window.partitionBy("iu_ac", "year", "month", "hour")
    val windowDateRank = Window.partitionBy("year", "month", "day", "hour")
      .orderBy("trafic_avg_per_hour", "trafic_max_per_hour", "trafic_stddev_per_hour", "trafic_variance_per_hour")
    val windowRank = Window.orderBy("avg_trafic_percentile_per_day_hour")



    df.withColumn("hour", substring(col("t_1h"), 12, 2))
      .withColumn("etat_trafic_int",
          when(col("etat_trafic") === "Fluide", 1)
            .when(col("etat_trafic") === "Pré-saturé", 2)
            .when(col("etat_trafic") === "Saturé", 3)
            .when(col("etat_trafic") === "Bloqué", 4)
            .otherwise(0)
      )
      .filter(col("etat_trafic_int") =!= 0)
      .withColumn("trafic_avg_per_hour", avg("etat_trafic_int") over windowIUAC)
      .withColumn("trafic_max_per_hour", max("etat_trafic_int") over windowIUAC)
      .withColumn("trafic_min_per_hour", min("etat_trafic_int") over windowIUAC)
      .withColumn("trafic_variance_per_hour", variance("etat_trafic_int") over windowIUAC)
      .withColumn("trafic_stddev_per_hour", stddev("etat_trafic_int") over windowIUAC)
      .withColumn("trafic_percentile_per_hour", percent_rank() over windowDateRank)
      .join(joinDf, "iu_ac")
      .groupBy("day", "hour").agg(
        avg("trafic_percentile_per_hour").as("avg_trafic_percentile_per_day_hour"),
        avg("trust").as("avg_trust")
      )
      .withColumn("trafic_percentile", percent_rank() over windowRank)
      .withColumn("is_good_time_to_drive",
        when(col("trafic_percentile") < 0.25, "very good")
          .when(col("trafic_percentile").between(0.25, 0.5), "correct")
          .when(col("trafic_percentile").between(0.5, 0.75), "not a good idea")
          .when(col("trafic_percentile") > 0.75, "take a bike")
          .otherwise(0)
      )

    df.write.partitionBy("day", "hour").mode(SaveMode.Overwrite).parquet(outputFile)
  }
}