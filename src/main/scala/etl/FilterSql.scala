package etl

import java.sql.Timestamp

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

object FilterSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getCanonicalName).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sc.addJar("/home/xdl/loganalyse/out/artifacts/loganalyse_jar/loganalyse.jar")
    val lines = sc.textFile("hdfs://master:9000/user/xdl/data/access.log")
    val loginfos = lines.map(line=>{
      //1.正则匹配
        ETLUtil.regexMatch(line).get
    })
    //ETL1
    //从文件中读取对应区域的ip地址范围,设置为广播变量
    val iptoArea = sc.textFile("hdfs://master:9000/user/xdl/data/iptoArea.dat")
    val ipStandard :Array[(Long,Long,String)] = iptoArea.map(text=>{
      val msg = text.split("\\s+")
      (ETLUtil.ipTo256Long(msg(0)),ETLUtil.ipTo256Long(msg(1)),msg(2))
    }).collect().sortBy(_._1)
    val ipbroadcast = sc.broadcast(ipStandard)
    //数据清洗
    //Row
    val loginfoRow = loginfos
      //2.过滤URL中下载文件的无用行
      .filter(obj=>ETLUtil.urlFilter(obj.url))
      //3、对ip进行转换（保留IP）
      .map(x=>Row(x.ip,
                  ETLUtil.ipOfArea(x.ip,ipbroadcast.value),
                  x.sessionid,
                  Timestamp.valueOf(x.time),
                  x.time.getYear.toString,
                  x.time.getMonthValue.toString,
                  x.time.getDayOfMonth.toString,
                  x.url,
                  x.status,
                  x.sentBytes,
                  x.referer,
                  x.userAgent))
    //Schema
    val schema = StructType( StructField("ip", StringType, false)
                          :: StructField("district",  StringType, false)
                          :: StructField("sessionid",  StringType, false)
                          :: StructField("time", TimestampType, false)
                          :: StructField("yearstr", StringType, false)
                          :: StructField("monthstr", StringType, false)
                          :: StructField("daystr", StringType, false)
                          :: StructField("url", StringType, false)
                          :: StructField("status", StringType, false)
                          :: StructField("sentBytes", StringType, false)
                          :: StructField("referer", StringType, false)
                          :: StructField("userAgent", StringType, false)::  Nil)
    //DF
    val dflog = spark.createDataFrame(loginfoRow,schema)

    dflog.createOrReplaceTempView("log")
    //创建ods(贴源层)分区表
    spark.sql("""set hive.exec.dynamic.partition=true""")
    spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
    spark.sql(
      """insert overwrite table sparktest.loginfo_partition partition(yearstr,monthstr,daystr)
        |select ip,district,sessionid,time,url,status,sentBytes,referer,userAgent,yearstr,monthstr,daystr from log""".stripMargin)
    spark.stop()
  }
}
