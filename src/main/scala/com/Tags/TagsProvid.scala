package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 渠道标签
  */
object TagsProvid extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    //获取渠道ID
    val value:String = row.getAs("adplatformproviderid")
    if (StringUtils.isNotBlank(value)) {
      list:+=("CN"+value,1)
    }
    list
  }
}
