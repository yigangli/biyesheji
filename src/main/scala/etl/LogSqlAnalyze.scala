package etl

import org.apache.spark.sql.SparkSession

object LogSqlAnalyze {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getCanonicalName).enableHiveSupport().getOrCreate()
    spark.sparkContext.addJar("/home/xdl/loganalyse/out/artifacts/loganalyse_jar/loganalyse.jar")
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("use sparktest")
    //spark.sql("select count(*) as cnt from sparktest.loginfo2").show
    //如果用户浏览时间间隔超过30分钟，算2次独立的浏览记录
    import org.apache.spark.sql._
    import java.util.Properties
    val prop = new Properties()
    prop.put("user", "hive")
    prop.put("password", "hive")
    prop.put("driver","com.mysql.jdbc.Driver")
    //按终端类型统计
//    spark.sql("select pcormove,count(pcormove) as cnt  from sparktest.loginfoetl2 left outer join sparktest.dim_pcormove on loginfoetl2.useragent=dim_pcormove.id group by pcormove").show
//     按栏目统计
//    spark.sql("select topic,count(topic) as cnt  from sparktest.loginfoetl2 left outer join sparktest.dim_topic on loginfoetl2.url=dim_topic.id group by topic").show
//     按地域统计
//    spark.sql("select location,count(location) as cnt  from sparktest.loginfoetl2 left outer join sparktest.dim_location on loginfoetl2.district=dim_location.id group by location").show
//     按来源统计
//    spark.sql("select dim_source.referer,count(dim_source.referer) as cnt  from sparktest.loginfoetl2 left outer join sparktest.dim_source on loginfoetl2.referer=dim_source.id group by dim_source.referer").show

    val df_fact = spark.sql(
      """
         select timeid,district as districtid,referer as refererhostid,userAgent as devicetypeid,url as columnid,1 as pv from loginfoetl2
      """.stripMargin)
    df_fact.createOrReplaceTempView("df_fact")
    //按终端类型统计
    spark.sql("""select distinct b.pcormove,sum(pv)over(partition by b.pcormove) as sum_pv from df_fact a inner join dim_pcormove b on a.devicetypeid=b.id""").write.mode(SaveMode.Append).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.pv_pcormove", prop)
//    spark.sql("""select b.pcormove,count(*) as sum_pv from df_fact a inner join dim_pcormove b on a.devicetypeid=b.id group by b.pcormove""").show(2000)
//        按栏目统计
    spark.sql("""select distinct b.topic,sum(pv)over(partition by b.topic) as sum_pv from df_fact a inner join dim_topic b on a.columnid=b.id""").write.mode(SaveMode.Append).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.pv_topic", prop)
//    spark.sql("""select distinct b.topic,count(*)over(partition by b.topic) as sum_pv from df_fact a inner join dim_topic b on a.columnid=b.id""").show(2000)
    //按地域统计
    spark.sql("""select distinct b.location,sum(pv)over(partition by b.location) as sum_pv from df_fact a inner join dim_location b on a.districtid=b.id""").write.mode(SaveMode.Append).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.pv_location", prop)
    //按来源统计
    spark.sql("""select distinct b.referer,sum(pv)over(partition by b.referer) as sum_pv from df_fact a inner join dim_source b on a.refererhostid=b.id""").write.mode(SaveMode.Append).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.pv_referer", prop)
    //按hour统计
    spark.sql("""select distinct b.hour,sum(pv)over(partition by b.hour) as sum_pv from df_fact a inner join dim_time b on a.timeid=b.id order by b.hour""").write.mode(SaveMode.Append).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.pv_hour", prop)
//    访问分析
    // 每天最热门的栏目Top3
    spark.sql("""
             select topic,sum_pv,rank()over(order by sum_pv desc)as rank
                |from (select distinct b.topic,sum(pv)over(partition by b.topic) as sum_pv from df_fact a inner join dim_topic b on a.columnid=b.id) limit 3
                |""".stripMargin).write.mode(SaveMode.Append).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.pv_topic_rank", prop)
    // 各种维度的访问分析
    //most pv per hour
    spark.sql("""
             select hour,sum_pv,rank()over(order by sum_pv desc)as rank
                |from (select distinct b.hour,sum(pv)over(partition by b.hour) as sum_pv from df_fact a inner join dim_time b on a.timeid=b.id) limit 3
                |""".stripMargin).write.mode(SaveMode.Append).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.pv_hour_rank", prop)
    spark.stop()

  }
}
