package etl

import org.apache.spark.sql.SparkSession

object Filter3Sql_UV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getCanonicalName).enableHiveSupport().getOrCreate()
    spark.sparkContext.addJar("/home/xdl/loganalyse/out/artifacts/loganalyse_jar/loganalyse.jar")
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("use sparktest")
    import org.apache.spark.sql._
    import java.util.Properties
    val prop = new Properties()
    prop.put("user", "hive")
    prop.put("password", "hive")
    prop.put("driver","com.mysql.jdbc.Driver")
    val df_fact1 = spark.sql(
      """
         select sessionid,time,timeid,district as districtid,referer as refererhostid,userAgent as devicetypeid,url as columnid from loginfoetl2
      """.stripMargin)
    df_fact1.createOrReplaceTempView("df_fact1")
    // 用户访问时长、访问步长
    val df_pvtime = spark.sql("""
             select sessionid,time from df_fact1 a inner join dim_time b on a.timeid=b.id
              """.stripMargin)
    import spark.implicits._
    df_pvtime.rdd.map(row => (row.getString(0),row.getTimestamp(1))).groupByKey().map(x=>{
      (x._1,x._2.toArray.sortBy(_.getTime))
    }).map(x=>{
      var arr = x._2.toArray.head+:x._2.toArray
      val len = arr.length-1
      val indexs = (1 to len).map(index =>{
        if((arr(index).getTime-arr(index-1).getTime)/60000>30){
          Some(index)
        }else{
          None
        }
      }).filter(_.isDefined)
      var resArr=Array(arr)
      if(indexs.isEmpty){
        resArr
      }else {
        resArr = indexs.indices.map(x => {
          val l = if(x==0){indexs(x).get}
          else{
            indexs(x).get-indexs(x-1).get
          }
          val newarr = arr.take(l)
          arr = arr.drop(l)
          newarr
        }).toArray
      }
      resArr(0) = resArr(0).drop(1)
      (x._1,resArr)
    })//.foreach(x=>println(x._1,x._2.map(_.toBuffer).toBuffer))
      // RDD[(String, Array[Array[Timestamp]])]
      .map(x=>{
      x._2.map(time=>{
        //        (x._1,time.length,(time(time.length-1).getTime-time(0).getTime)/60000)
        val len=(time(time.length-1).getTime-time(0).getTime)
        val lenstr = s"${len/60000}m${len%60000/1000}s"
        UserViewLength(x._1,time.length,lenstr)
      })
    }).flatMap(x=>{x}).toDS().write.mode(SaveMode.Append).jdbc("jdbc:mysql://master:3306/spark?useUnicode=true&characterEncoding=utf8", "spark.uv_long", prop)
    //各种维度的访问时长、访问步长
    spark.stop()
  }
}
