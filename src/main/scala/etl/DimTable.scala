package etl

import org.apache.spark.sql.{SaveMode, SparkSession}

object DimTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getCanonicalName).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
//    val a = spark.read.option("inferschema",true).option("delimiter"," ").option("header",false).csv("hdfs://master:9000/user/xdl/data/referer.dat").toDF("id","referer")
//    a.createOrReplaceTempView("a")
//    a.show()
//    val b = spark.read.option("inferschema",true).option("delimiter"," ").option("header",false).csv("hdfs://master:9000/user/xdl/data/topic.dat").toDF("id","topic")
//    b.createOrReplaceTempView("b")
//    b.show()
//    val c = spark.read.option("inferschema",true).option("delimiter"," ").option("header",false).csv("hdfs://master:9000/user/xdl/data/pcormove.dat").toDF("id","pcormove")
//    c.createOrReplaceTempView("c")
//    c.show()
//    val d = spark.read.option("inferschema",true).option("delimiter"," ").option("header",false).csv("hdfs://master:9000/user/xdl/data/location.dat").toDF("id","location")
//    d.createOrReplaceTempView("d")
//    d.show()
//
//    spark.sql("use sparktest")
//    spark.sql("""create table if not exists dim_source(id int,referer string)stored as parquet location 'hdfs://master:9000/user/hive/source'""")
//    spark.sql("""create table if not exists dim_topic(id int,topic string)stored as parquet location 'hdfs://master:9000/user/hive/topic'""")
//    spark.sql("""create table if not exists dim_pcormove(id int,pcormove string)stored as parquet location 'hdfs://master:9000/user/hive/pcormove'""")
//    spark.sql("""create table if not exists dim_location(id int,location string)stored as parquet location 'hdfs://master:9000/user/hive/location'""")
//    spark.sql("""create table if not exists dim_time(id int,year string,month string,day string,hour int)stored as parquet location 'hdfs://master:9000/user/hive/time'""")
//    spark.sql("insert into dim_source select * from a")
//    spark.sql("insert into dim_topic select * from b")
//    spark.sql("insert into dim_pcormove select * from c")
//    spark.sql("insert into dim_location select * from d")
//    spark.sql("""insert overwrite table dim_time select row_number()over(order by hour(time))as id,yearstr,monthstr,daystr,hour(time)as hour from sparktest.loginfoetl2 group by yearstr,monthstr,daystr,hour order by hour""")
    import org.apache.spark.sql._
    import java.util.Properties
    val prop = new Properties()
    prop.put("user", "hive")
    prop.put("password", "hive")
    prop.put("driver","com.mysql.jdbc.Driver")
    spark.sql("use sparktest")
    spark.sql("""select * from dim_location""").write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.dim_location", prop)
    spark.sql("""select * from dim_topic""").write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.dim_topic", prop)
    spark.sql("""select * from dim_time""").write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.dim_time", prop)
    spark.sql("""select * from dim_source""").write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.dim_source", prop)
    spark.sql("""select * from dim_pcormove""").write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.dim_pcormove", prop)
    spark.stop()
  }
}
