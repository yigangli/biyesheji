package etl

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern

object ETLUtil {
  //1.正则匹配
  val regex = """(\S+)\s+(\S+)\s+\[(.+)\]\s+\"(.+)\"\s+(\d+)\s+(\d+)\s+\"(.+)\"\s+\"(.+)\""""
  val pattern = Pattern.compile(regex)
  def regexMatch(str:String):Option[LogInfo]={
    val matcher = pattern.matcher(str)
    if(matcher.matches()){
      Some(LogInfo(matcher.group(1),matcher.group(2),LocalDateTime.parse(matcher.group(3),DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")),matcher.group(4),matcher.group(5),matcher.group(6),matcher.group(7),matcher.group(8)))
    }else{
      None
    }
  }
  //2.过滤URL中下载文件的无用行
  def urlFilter(msg:String):Boolean={
    val field = msg.split("\\s+")
    if(field.length>1) {
      !(field(1).matches("""\S+.png""") | field(1).matches("""\S+.jpg""") | field(1).matches("""\S+.css""") | field(1).matches("""\S+.js""") | field(1).matches("""\S+.ico""") | field(1).matches("""\S+.gif""") | field(1).matches("""/wp-\S+""") | field(1).matches("""/css/"""))
    }else{
      true
    }
  }
  //将ip地址转换为256进制的数
  def ipTo256Long(ip:String):Long = {
    val ips = ip.split("\\.")
    ips.map(num=>num.toLong).zipWithIndex
      .map(num=>(num._1*math.pow(256,3-num._2)).toLong).sum
  }

  //将日志数据中userAgent与参考文件对应,转换成相应userAgent名称
  def userAgentofColumn(obj: LogInfo, broadcast:Array[(String, String)]): Unit ={
    broadcast.foreach(x=>{
      if(x._1.equals(obj.userAgent)){
        obj.userAgent=x._2
      }
    })
  }
  def userAgentofColumn(obj: LogInfoETL2, broadcast:Array[(String, String)]): Unit ={
    broadcast.foreach(x=>{
      if(x._1.equals(obj.userAgent)){
        obj.userAgent=x._2
      }
    })
  }

  //将日志数据中URL与其对应,转换成相应栏目名称
  def urlOfColumn(obj:LogInfoETL2,urlbroadcast:Array[(String, String)]): Unit ={
    val field = obj.url.split("\\s+")
    if(field.length>1)
      urlbroadcast.foreach(x=>{
        if(field(1).startsWith(x._2)){
          obj.url = x._1
        }
      })
  }
  //转换ip所属区域
  def ipOfArea(ip:String,ipBroadcast:Array[(Long, Long, String)]):String={
    val ipValue = ETLUtil.ipTo256Long(ip)
    val res = ipBroadcast.map(x=>{
      if(ipValue>=x._1&ipValue<=x._2){
        x._3
      }else{
        ""
      }
    }).filter(_!="")
    if(res.length>0){
      res.head
    }else{
      ip
    }
  }
}
