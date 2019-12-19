package etl.dwd

import java.sql.Timestamp

import etl.ETLUtil
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object DWDLogAnalyse {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getCanonicalName).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val sc = spark.sparkContext
    val odsDf = spark.sql("SELECT * FROM LOG_ODS.LOGANALYSE_ODS")
    val ipfile = sc.textFile("hdfs://master:9000/ip.txt")
    val ipStandard :Array[(Long,Long,String)] = ipfile.map(text=>{
      val msg = text.split("\\s+")
      (ETLUtil.ipTo256Long(msg(0)),ETLUtil.ipTo256Long(msg(1)),msg(2))
    }).collect().sortBy(_._1)
    val ipbroadcast = sc.broadcast(ipStandard)
    def iptoArea = udf{
      (ip:String)=>{
        ETLUtil.ipOfArea(ip,ipbroadcast.value)
      }
    }
    val dwdDf = odsDf.withColumn("district",iptoArea($"ip")).
      withColumn("year",substring($"time",0,4)).
      withColumn("month",substring($"time",6,2)).
      withColumn("day",substring($"time",9,2))
    dwdDf.createOrReplaceTempView("log")
    //创建ods(贴源层)分区表
    spark.sql("""SET HIVE.EXEC.DYNAMIC.PARTITION=TRUE""")
    spark.sql("""SET HIVE.EXEC.DYNAMIC.PARTITION.MODE=NONSTRICT""")
    spark.sql(
      """CREATE TABLE IF NOT EXISTS LOG_DWD.LOGANALYSE_DWD(IP STRING ,DISTRICT STRING,SESSIONID STRING,TIME STRING,URL STRING,
        |STATUS STRING,SENTBYTES STRING,REFERER STRING,USERAGENT STRING) PARTITIONED BY(YEAR STRING,MONTH STRING,DAY STRING)""".stripMargin)
    spark.sql(
      """INSERT OVERWRITE TABLE LOG_DWD.LOGANALYSE_DWD PARTITION(YEAR,MONTH,DAY)
        |SELECT IP,
        |DISTRICT,
        |SESSIONID,
        |TIME,
        |URL,
        |STATUS,
        |SENTBYTES,
        |REFERER,
        |USERAGENT,
        |YEAR,
        |MONTH,
        |DAY FROM LOG""".stripMargin)
    spark.stop()
  }

}
