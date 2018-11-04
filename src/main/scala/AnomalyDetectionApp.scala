import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.reflect.io.Directory



object AnomalyDetectionApp {

  def main(args:Array[String]): Unit =
  {

    val spark = SparkSession.builder()
          .appName("My App")
          .master("local[*]")
          .getOrCreate()


    spark.conf.set("spark.sql.streaming.checkpointLocation","tmp/")

    val entrySchema = new StructType().add("time", "Timestamp").add("count", "integer")

    val linesDf =
      spark
        .readStream                       // `readStream` instead of `read` for creating streaming DataFrame
        .schema(entrySchema)               // Set the schema of the JSON data
        .option("maxFilesPerTrigger", 1)  // Treat a sequence of files as a stream by picking one file at a time
        .csv("data/")

    val LookbackWindowMinutes = 120
    val PercentAnomalyThreshold = 75
    val avgOverLookbackColumnName= s"avg_over_past_${LookbackWindowMinutes}_min"


    val slidingWindow =  Window
          .orderBy(asc("time"))
          .rowsBetween(-LookbackWindowMinutes, 0)


    val slidingWindowtest = window(col("time"), "1 minute", "120 minutes")


          linesDf
          .withColumn(avgOverLookbackColumnName,avg(col("count")).over(slidingWindow))
          .withColumn("deviation",col("count")-col(avgOverLookbackColumnName))
          .withColumn("percent_deviation",abs(col("deviation")/col(avgOverLookbackColumnName)*lit("100")))
          .withColumn("date",col("time").substr(0,10))
          .withColumn("anomaly_flag",when(col("percent_deviation")>=PercentAnomalyThreshold,1))
          .withColumn("anomalous_timestamp",when(col("anomaly_flag").isNotNull,col("time")))
          .groupBy(col("date"))
          .agg(count(col("anomaly_flag")).alias("anomaly_count"),concat_ws(", ",collect_list(col("anomalous_timestamp"))).alias("anomalous_timestamps"))
          .withColumn("anomaly_status", when(col("anomaly_count")>=5,lit("anomaly detected")).otherwise(lit("no anomalies")))
          .coalesce(1)
//          .write
//          .option("header", "true").csv("AnomalyOutput")

    // Is this DF actually a streaming DF?
    linesDf.isStreaming

    val stream = linesDf.writeStream.format("csv").start("out.csv")
    stream.awaitTermination()

//
//
//
//
//
//
//
//    val directory = new Directory(new File("AnomalyOutput"))
//    directory.deleteRecursively()
//
//
//    val conf = new SparkConf().setAppName("My App").setMaster("local[*]")
//    val spark = SparkSession.builder()
//      .appName("My App")
//      .master("local[*]")
//      .getOrCreate()
//
//    val linesDf = spark.read.option("header", "true").csv("all.csv")
//    val LookbackWindowMinutes = 120
//    val PercentAnomalyThreshold = 75
//
//    val slidingWindow =  Window
//      .orderBy(asc("time"))
//      .rowsBetween(-LookbackWindowMinutes, 0)
//
//    val avgOverLookbackColumnName= s"avg_over_past_${LookbackWindowMinutes}_min"
//
//
//    linesDf
//      .withColumn(avgOverLookbackColumnName,avg(col("count")).over(slidingWindow))
//      .withColumn("deviation",col("count")-col(avgOverLookbackColumnName))
//      .withColumn("percent_deviation",abs(col("deviation")/col(avgOverLookbackColumnName)*lit("100")))
//      .withColumn("percent_deviation",abs(col("deviation")/col(avgOverLookbackColumnName)*lit("100")))
//      .withColumn("date",col("time").substr(0,10))
//      .withColumn("anomaly_flag",when(col("percent_deviation")>=PercentAnomalyThreshold,1))
//      .withColumn("anomalous_timestamp",when(col("anomaly_flag").isNotNull,col("time")))
//      .groupBy(col("date"))
//      .agg(count(col("anomaly_flag")).alias("anomaly_count"),concat_ws(", ",collect_list(col("anomalous_timestamp"))).alias("anomalous_timestamps"))
//      .withColumn("anomaly_status", when(col("anomaly_count")>=5,lit("anomaly detected")).otherwise(lit("no anomalies")))
//      .coalesce(1)
//      .write
//      .option("header", "true").csv("AnomalyOutput")
//
//    spark.stop()

  }
}
