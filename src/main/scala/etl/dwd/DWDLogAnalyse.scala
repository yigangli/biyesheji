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
      withColumn("time",substring($"time",0,10)).
      withColumn("year",substring($"time",0,4)).
      withColumn("month",substring($"time",6,2)).
      withColumn("day",substring($"time",9,2)).
      withColumn("referer",formatReferer($"referer"))
    dwdDf.createOrReplaceTempView("log")
    //创建ods(贴源层)分区表
    spark.sql("""set hive.exec.dynamic.partition=true""")
    spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
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
  def formatReferer: UserDefinedFunction =udf{
    (str:String)=>{
      val baidur = """(.+baidu.+)""".r
      val googler = """(.+google.+)""".r
      val weibor = """(.+weibo.+)""".r
      val rmbr = """(.+100rmb.+)""".r
      val blogr = """(.+blog.+)""".r
      val cnodejsr = """(.+cnodejs.+)""".r
      val weixinr = """(.+weixin.+)""".r
      val duowanr = """(.+duowan.+)""".r
      val cloudr = """(.+cloud.+)""".r
      str match {
        case baidur(_) => "baidu"
        case googler(_) => "google"
        case weibor(_) => "weibo"
        case rmbr(_) => "100rmb"
        case blogr(_) => "blog"
        case cnodejsr(_) => "cnodejs"
        case weixinr(_) => "weixin"
        case duowanr(_) => "duowan"
        case cloudr(_) => "cloud"
        case _ => "other"
      }
    }
  }

}
