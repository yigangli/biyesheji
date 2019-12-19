package etl

import java.time.LocalDateTime

case class LogInfo(ip:String,sessionid:String,time:String,url:String,status:String,sentBytes:String,referer:String,var userAgent:String)
