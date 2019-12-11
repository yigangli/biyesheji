package etl

import java.time.LocalDateTime

case class LogInfo(var ip:String,sessionid:String,time:LocalDateTime,var url:String,status:String,sentBytes:String,referer:String,var userAgent:String)
