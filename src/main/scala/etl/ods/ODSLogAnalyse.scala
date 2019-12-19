package etl.ods

import java.util.regex.Pattern

import etl.{ETLUtil, LogInfo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ODSLogAnalyse {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getCanonicalName).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val sc = spark.sparkContext
    val lines = sc.textFile("hdfs://master:9000/access01.log")
    val loginfos :RDD[LogInfo] = lines.map(line => {
      //1.正则匹配
      regexMatch(line).get
    })
    val odsDf = loginfos.toDF()
    ETLUtil.saveData(odsDf,"LOG_ODS","LOGANALYSE_ODS")
  }

  //1.正则匹配
  def regexMatch(str:String):Option[LogInfo]={
    val regex = """(\S+)\s+(\S+)\s+\[(.+)\]\s+\"(.+)\"\s+(\d+)\s+(\d+)\s+\"(.+)\"\s+\"(.+)\""""
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(str)
    if(matcher.matches()){
      Some(LogInfo(matcher.group(1),matcher.group(2),matcher.group(3),matcher.group(4),matcher.group(5),matcher.group(6),matcher.group(7),matcher.group(8)))
    }else{
      None
    }
  }
}
