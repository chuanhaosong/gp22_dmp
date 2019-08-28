package com.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将字段文件数据，存储到Redis中
  */
object APP2Jedis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc  = new SparkContext(conf)

    //读取字段文件
    val dict = sc.textFile("dir/app_dict.txt")
    //处理字段文件
    dict.map(_.split("\t",-1))
      .filter(_.length>=5).foreachPartition(arr =>{
      val jedis = JedisConnectionPool.getConnection()
      arr.foreach(arr => {
        jedis.set(arr(4),arr(1))
      })
      jedis.close()
    })
    sc.stop()

  }
}
