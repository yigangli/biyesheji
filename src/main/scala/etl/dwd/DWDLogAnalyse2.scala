package etl.dwd

import org.apache.spark.sql.SparkSession

object DWDLogAnalyse2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getCanonicalName).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val sc = spark.sparkContext
    val dwdDf = spark.sql("SELECT * FROM LOG_DWD.LOGANALYSE_DWD")
    //1、过滤4XX、5XX
    val clearDf = dwdDf.filter("CAST(STATUS AS INT)<400").
    // 2、丢掉sentbytes
    drop("sentBytes","status")
    val userAgent = spark.sparkContext.textFile("hdfs://master:9000/userAgent.txt")
  }
}
