package etl

import org.apache.spark.sql.{SaveMode, SparkSession}

object Filter3Sql_PV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getCanonicalName).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.addJar("/home/xdl/loganalyse/out/artifacts/loganalyse_jar/loganalyse.jar")
    spark.sql("use sparktest")
    import org.apache.spark.sql._
    import java.util.Properties
    val prop = new Properties()
    prop.put("user", "hive")
    prop.put("password", "hive")
    prop.put("driver","com.mysql.jdbc.Driver")
    spark.sql("""select timeid,district as districtid,referer as refererhostid,userAgent as devicetypeid,url as columnid,1 as pv from loginfoetl2""").write.mode(SaveMode.Append).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.pv_fact", prop)
    spark.stop()
  }
}
