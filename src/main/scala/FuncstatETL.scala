import java.time.{LocalDateTime, ZoneId}
import java.util.{Calendar, Properties}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object FuncstatETL {
  def main(args: Array[String]): Unit = {
      val sparksession = SparkSession.builder().master("local[*]").appName("FuncstatETL").getOrCreate()
      val rawlogs: Dataset[String] = sparksession.read.textFile("/Users/yangwensheng/IdeaProjects/xuri_logclean/log")
      import sparksession.implicits._
      val cleanedlogs = rawlogs
      .filter(_.contains("[end request:]"))
      .map(line => {
        val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS")
        val items = line.split("\\|\\|")
        val rawip = items(0)
        val ip = rawip.substring(rawip.indexOf("(") + 1, rawip.indexOf(")"))
        val dateTime = extractContentInBracket(items(1))
        val formatedDate = LocalDateTime.ofInstant(dateFormat.parse(dateTime).toInstant, ZoneId.systemDefault())
        val timestamp = dateFormat.parse(dateTime).getTime
        val hour = formatedDate.getHour
        val min = formatedDate.getMinute
        val sec = formatedDate.getSecond
        val ms = formatedDate.getNano
        val sessionid = extractContentInBracket(items(3))
        val servername = extractContentInBracket(items(4))
        val funcurl = extractContentInBracket(items(5))
        val opid = items(6).split("\\[")(1)
        val optype = items(6).split("\\[")(2).substring(0,1)
        val userip = extractContentInBracket(items(7))
        val accountid = extractContentInBracket(items(8))
        val agent = extractContentInBracket(items(9))
        val requestquerystring = extractContentInBracket(items(10))
        val referer = extractContentInBracket(items(11))
        val delay = extractContentInBracket(items(12)) //[77] 有可能是null
        val anchor = extractContentInBracket(items(13))
        val ref = extractContentByKey(extractContentInBracket(items(10)), "ref=")
        val requestId = extractContentByKey(extractContentInBracket(items(13)), "requestId=")
        val param = ""
        val bpm = extractContentByKey(extractContentInBracket(items(10)), "bpm=")
        val date = extractContentInBracket(items(1)).substring(0, 10)
//        timestamp+"#"+ hour+"#"+ min+"#"+ sec+"#"+ ms+"#"+ sessionid+"#"+ servername+"#"+ funcurl+"#"+ opid+"#"+ optype+"#"+ userip+"#"+ accountid+"#"+ agent+"#"+ requestquerystring+"#"+ referer+"#"+ delay+"#"+ anchor+"#"+ ref+"#"+ requestId+"#"+ param+"#"+ bpm+"#"+ date
//        opid+"#"+optype
        (timestamp, hour, min, sec, ms, sessionid, servername, funcurl, opid, optype, userip, accountid, agent, requestquerystring, referer, delay, anchor, ref, requestId, param, bpm, date)
      })
      val df: DataFrame = cleanedlogs.toDF()
    df.createTempView("funcstat")
    df.printSchema()
    val sql:String=
      s"""
        select * from funcstat
      """
    sparksession.sql("")
      val config: Config = ConfigFactory.load()
      val properties = new Properties()
      properties.setProperty("user",config.getString("db.default.user"))
      properties.setProperty("password",config.getString("db.default.password"))
      val dburl=config.getString("db.default.url")
      val dbtable=config.getString("db.default.table")
//      cleanedlogs.write.mode(SaveMode.Overwrite).text("/Users/yangwensheng/IdeaProjects/xuri_logclean/result")
      cleanedlogs.write.mode(SaveMode.Overwrite).jdbc(dburl,dbtable,properties)

    sparksession.stop()
  }
  def extractContentInBracket(rawContent:String):String={
    if(StringUtils.isNotBlank(rawContent)){
      rawContent.substring(rawContent.indexOf("[")+1,rawContent.indexOf("]"))
    }else{
      "NULL"
    }
  }
  def extractContentByKey(rawContent: String,key:String):String={
    if(rawContent.contains(key)){
      rawContent.split(key)(1).split("&")(0)
    }else{
      "NULL"
    }
  }
}
case class Funcstat(timestamp:Long, hour:Int, min:Int, sec:Int, ms:Int, sessionid:String,
                    servername:String, funcurl:String, opid:String, optype:String, userip:String, accountid:String,
                    agent:String, requestquerystring:String, referer:String, delay:String, anchor:String, ref:String,
                    requestId:String, param:String, bpm:String, date:String)