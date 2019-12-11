package etl

import java.sql.Timestamp

case class LogInfoETL2(ip:String,district:String,sessionid:String,time:Timestamp,var url:String,referer:String,var userAgent:String,yearstr:String,monthstr:String,daystr:String)
