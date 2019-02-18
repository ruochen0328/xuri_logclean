import java.time.temporal.IsoFields
import java.time.{LocalDateTime, ZoneId}

import FuncstatETL.{extractContentByKey, extractContentInBracket}
import org.apache.commons.lang3.time.FastDateFormat

object Demo {
  def main(args: Array[String]): Unit = {
    val line="ip(10.144.34.84):]||[2019-02-14 23:00:41.420]||[end request:]||[aaa42LeSSQQc5809zE6Hw]||[xuri.p4p.sogou.com]||[/report/hour/queryHourReport.action!]||[389[6:账户管家为推广服务客户操作]]||[116.226.1.223]||[18927200]||[Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0]||[?suf_p4p_user_id=18927200&reportQueryFormDto.endDate=2019-02-14&reportQueryFormDto.specialTypes={\"doCompareDate\":0,\"level\":\"1\",\"aggregationType\":\"1\"}&reportQueryFormDto.startDate=2019-02-14&ref=account,notCompare&t=1550156466789&reportQueryFormDto.deviceType=0&bpm=93144203.665426286.934521548.628689569&]||[http://xuri.p4p.sogou.com/cpcadindex/init2.action?suf_p4p_user_id=18927200]||[77]||[#/datareport/timeInterval/list/~requestId=c3a03189-3f2a-4ec9-8b26-724b210f6ae5&]||["
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS")
    val items = line.split("\\|\\|")
    val rawip=items(0)
    val ip = rawip.substring(rawip.indexOf("(")+1,rawip.indexOf(")"))
    val dateTime=extractContentInBracket(items(1))
    val formatedDate = LocalDateTime.ofInstant(dateFormat.parse(dateTime).toInstant,ZoneId.systemDefault())
    val timestamp = dateFormat.parse(dateTime).getTime
    val hour = formatedDate.getHour
    val min = formatedDate.getMinute
    val sec = formatedDate.getSecond
    val ms = formatedDate.getNano
    val sessionid = extractContentInBracket(items(3))
    val servername = extractContentInBracket(items(4))
    val funcurl = extractContentInBracket(items(5))
    val opid = items(6).split("\\[")(1)
    val optype = items(6).split("\\[")(2).charAt(0)
    val userip = extractContentInBracket(items(7))
    val accountid = extractContentInBracket(items(8))
    val agent = extractContentInBracket(items(9))
    val requestquerystring = extractContentInBracket(items(10))
    val referer = extractContentInBracket(items(11))
    val delay = extractContentInBracket(items(12)) //[77] 有可能是null
    val anchor = extractContentInBracket(items(13))
    val ref = extractContentByKey(extractContentInBracket(items(10)),"ref=")
    val requestId = extractContentByKey(extractContentInBracket(items(13)),"requestId=")
    val param = ""
    val bpm = extractContentByKey(extractContentInBracket(items(10)),"bpm=")
    val date = extractContentInBracket(items(1)).substring(0,10)
    print( ref, requestId, param, bpm, date)
  }
  def extractRef(str: String):String={
    if(str.contains("ref=")){
      str.split("ref=")(1).split("&")(0)
    }else{
      "NULL"
    }
  }
}
