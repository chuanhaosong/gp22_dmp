package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object TagsLocation extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    //获取省名
    val provincename:String = row.getAs("provincename")
    if (StringUtils.isNotBlank(provincename)){
      list:+=("ZP"+provincename,1)
    }
    //获取市名
    val cityname:String = row.getAs("cityname")
    if (StringUtils.isNotBlank(cityname)) {
      list:+=("ZC"+cityname,1)
    }

    list
  }
}
