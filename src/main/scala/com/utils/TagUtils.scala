package com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


/**
  * 标签工具类
  */
object TagUtils {

  //先过滤需要的字段
  val OnUserId=
    """
      | imei != '' or mac != '' or openudid != '' or androidid != '' or idfa != '' or
      | imeimd5 != '' or macmd5 != '' or openudidmd5 != '' or androididmd5 != '' or idfamd5 != '' or
      | imeisha1 != '' or macsha1 != '' or openudidsha1 != '' or androididsha1 != '' or idfasha1 != ''
    """.stripMargin

  //取出唯一不为空的ID
  def getOnUserId(row:Row):String={
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "IM" +v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => "MA" +v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => "OD" +v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) => "AD" +v.getAs[String]("androidid")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => "IF" +v.getAs[String]("idfa")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => "IMM" +v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => "MAM" +v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => "ODM" +v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => "ADM" +v.getAs[String]("androididmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => "IFM" +v.getAs[String]("idfamd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => "IMS" +v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) => "MAS" +v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) => "ODS" +v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => "ADS" +v.getAs[String]("androididsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => "IFS" +v.getAs[String]("idfasha1")
    }
  }

  //获取全部ID
  def getAllUserId(row: Row):List[String] = {
    var list = List[String]()
    if(StringUtils.isNotBlank(row.getAs[String]("imei"))) list:+="IM : " + row.getAs[String]("imei")
    if(StringUtils.isNotBlank(row.getAs[String]("mac"))) list:+="MA : " + row.getAs[String]("mac")
    if(StringUtils.isNotBlank(row.getAs[String]("openudid"))) list:+="OD : " + row.getAs[String]("openudid")
    if(StringUtils.isNotBlank(row.getAs[String]("androidid"))) list:+="AD : " + row.getAs[String]("androidid")
    if(StringUtils.isNotBlank(row.getAs[String]("idfa"))) list:+="IF : " + row.getAs[String]("idfa")
    if(StringUtils.isNotBlank(row.getAs[String]("imeimd5"))) list:+="IMM : " + row.getAs[String]("imeimd5")
    if(StringUtils.isNotBlank(row.getAs[String]("macmd5"))) list:+="MAM : " + row.getAs[String]("macmd5")
    if(StringUtils.isNotBlank(row.getAs[String]("openudidmd5"))) list:+="ODM : " + row.getAs[String]("openudidmd5")
    if(StringUtils.isNotBlank(row.getAs[String]("androididmd5"))) list:+="ADM : " + row.getAs[String]("androididmd5")
    if(StringUtils.isNotBlank(row.getAs[String]("idfamd5"))) list:+="IFM : " + row.getAs[String]("idfamd5")
    if(StringUtils.isNotBlank(row.getAs[String]("imeisha1"))) list:+="IMS : " + row.getAs[String]("imeisha1")
    if(StringUtils.isNotBlank(row.getAs[String]("macsha1"))) list:+="MAS : " + row.getAs[String]("macsha1")
    if(StringUtils.isNotBlank(row.getAs[String]("openudidsha1"))) list:+="ODS : " + row.getAs[String]("openudidsha1")
    if(StringUtils.isNotBlank(row.getAs[String]("androididsha1"))) list:+="ADS : " + row.getAs[String]("androididsha1")
    if(StringUtils.isNotBlank(row.getAs[String]("idfasha1"))) list:+="IFS : " + row.getAs[String]("idfasha1")
    list
  }



}
