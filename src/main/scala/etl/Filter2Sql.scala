package etl

import org.apache.spark.sql.SparkSession

object Filter2Sql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getCanonicalName).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.addJar("/home/xdl/loganalyse/out/artifacts/loganalyse_jar/loganalyse.jar")
    //ETL2
    //1、过滤4XX、5XX
    val df1 = spark.sql("select * from sparktest.loginfo_partition")
    val df2 = df1.filter("cast(status as int)<400")
    // 2、丢掉sentbytes
    val df3 = df2.drop("sentBytes","status")
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
    //clean
    val rdd4 = df3.rdd.map(x=>LogInfoETL2(x.getAs("ip"),x.getAs("district"),x.getAs("sessionid"),x.getAs("time"),x.getAs("url"),x.getAs("referer"),x.getAs("useragent"),x.getAs("yearstr"),x.getAs("monthstr"),x.getAs("daystr" )))
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
    //维度表参考
//    spark.sql(
//      """select row_number() over(order by url) as id,url from(
//         |select url from df group by url)
//      """.stripMargin).show
//    spark.sql(
//      """select row_number() over(order by useragent) as id,useragent from(
//        |select useragent from df group by useragent)
//      """.stripMargin).show
//    spark.sql(
//      """select row_number() over(order by district) as id,district from(
//        |select district from df group by district)
//      """.stripMargin).show
//    spark.sql(
//      """select referer from(
//        |select case when referer like '-' then '1'
//        |when referer like '%baidu%' then '2'
//        |when referer like '%google%' then '3'
//        |when referer like '%weibo%' then '4'
//        |when referer like '%100rmb%' then '5'
//        |when referer like '%blog%' then '6'
//        |when referer like '%cnodejs%' then '7'
//        |when referer like '%weixin%' then '8'
//        |when referer like '%duowan%' then '9'
//        |when referer like '%cloud%' then '10'
//        |else '0' end referer
//        |from df group by referer)
//      """.stripMargin).show(5000)
    spark.sql("use sparktest")
    val df_dim = spark.sql(
      """select ip,d2.id district,sessionid,time,d6.id url,case when referer like '-' then '1'
        |     when referer like '%baidu%' then '2'
        |     when referer like '%google%' then '3'
        |     when referer like '%weibo%' then '4'
        |     when referer like '%100rmb%' then '5'
        |     when referer like '%blog%' then '6'
        |     when referer like '%cnodejs%' then '7'
        |     when referer like '%weixin%' then '8'
        |     when referer like '%duowan%' then '9'
        |     when referer like '%cloud%' then '10'
        |     else '0' end referer,d3.id userAgent,d5.id timeid,yearstr,monthstr,daystr from df d1 left outer join dim_location d2 on d1.district=d2.location
        |left outer join dim_pcormove d3 on d1.userAgent=d3.pcormove
        |left outer join dim_time d5 on d5.year=d1.yearstr and d5.month=d1.monthstr and d5.day=d1.daystr and d5.hour=hour(d1.time)
        |left outer join dim_topic d6 on d1.url=d6 .topic
        |""".stripMargin)
    df_dim.createOrReplaceTempView("df_dim")
    spark.sql("""create table if not exists loginfoETL2(ip string,district string,sessionid string,time timestamp,url string,referer string,userAgent string,timeid string) partitioned by(yearstr string,monthstr string,daystr string)stored as parquet location'hdfs://master:9000/user/hive/loginfoETL2' """)
    spark.sql("""set hive.exec.dynamic.partition=true""")
    spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
    spark.sql("insert overwrite table loginfoETL2 partition(yearstr,monthstr,daystr) select * from df_dim")
    df_dim.repartition(1).write.format("csv").option("delimiter",",").option("header", true)
      .option("compression", "none").mode("overwrite").save("hdfs://master:9000/user/data/")
    spark.stop()
  }
}
