package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object TagsApp extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    val broad: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String,String]]]
    //获取APPID和APPname
    val appname: String = row.getAs("appname")
    val appid: String = row.getAs("appid")
    //空值判断
    if (StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if (StringUtils.isNoneBlank(appid)){
      list:+=("APP"+broad.value.getOrElse(appid,appid),1)
    }
    list
  }
}
