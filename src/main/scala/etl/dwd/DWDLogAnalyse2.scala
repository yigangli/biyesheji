package etl.dwd

import etl.{ETLUtil, LogInfoETL2}
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
    //读userAgent参考信息文件,设置为广播变量
    val userAgent = spark.sparkContext.textFile("hdfs://master:9000/userAgent.txt")
    val userAgenttransform = userAgent.map(line=>{
      val newline = line.replaceAll("\"","")
      if(newline.startsWith("User-Agent:")){
        val str = newline.replaceFirst("User-Agent:","").trim
        (str,"移动端")
      }else if(newline.startsWith("DNS")){
        (newline.trim,"开启域名监控")
      }else{
        (newline.trim,"PC端")
      }
    }).collect()
    val userAgentBroadCast = spark.sparkContext.broadcast(userAgenttransform)
    //读URL栏目信息文件,设置为广播变量
    val urlcolumn = spark.sparkContext.textFile("hdfs://master:9000/user/xdl/data/url.dat")
    val urltransform = urlcolumn.map(line=>{
      val field = line.split("\\s+")
      (field(0),field(1))
    }).collect()
    val urlbroadcast = spark.sparkContext.broadcast(urltransform)
    val rdd4 = clearDf.rdd.map(x=>LogInfoETL2(x.getAs("ip"),x.getAs("district"),x.getAs("sessionid"),x.getAs("time"),x.getAs("url"),x.getAs("referer"),x.getAs("useragent"),x.getAs("yearstr"),x.getAs("monthstr"),x.getAs("daystr" )))
      .map(obj=>{
        // 3、useragent > devicetypeid
        ETLUtil.userAgentofColumn(obj,userAgentBroadCast.value)
        // 4、referrer > Referehostid
        // 5、url > Columnid
        ETLUtil.urlOfColumn(obj,urlbroadcast.value)
        obj
      })
    import spark.implicits._
    val df = rdd4.toDS()
    df.createOrReplaceTempView("df")
  }
}
