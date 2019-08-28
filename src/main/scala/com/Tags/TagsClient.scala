package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * 设备标签
  */
object TagsClient extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析数据
    val row: Row = args(0).asInstanceOf[Row]
    //获取设备操作系统id
    val client: Int = row.getAs("client")
    client match {
      case 1 => list:+=("Android"+"D0001000"+client,1)
      case 2 => list:+=("IOS"+"D0001000"+client,1)
      case 3 => list:+=("WinPhone"+"D0001000"+client,1)
      case _ => list:+=("_其他"+"D00010004",1)
    }
    //获取设备联网方式名称
    val networkname:String = row.getAs("networkmannername")
    networkname match {
      case "Wifi" => list:+=("WIFI D00020001",1)
      case "4G" => list:+=("WIFI D00020002",1)
      case "3G" => list:+=("WIFI D00020003",1)
      case "2G" => list:+=("WIFI D00020004",1)
      case _  => list:+=("_D00020005",1)
    }
    //获取设备运营商名称
    val ispname: String = row.getAs("ispname")
    ispname match {
      case "移动" => list:+=(ispname+" D00030001",1)
      case "联通" => list:+=(ispname+" D00030002",1)
      case "电信" => list:+=(ispname+" D00030003",1)
      case _ => list:+=("_ D00030004",1)
    }
    list
  }
}
