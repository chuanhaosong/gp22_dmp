package com.ProCityCt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求指标二 统计各省市分布情况
  */
object proCity {
  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if(args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //创建一个集合保存数去输出路径
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //设置压缩方式，使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //读取本地文件
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    //注册临时表
    df.registerTempTable("log")
    //指标统计
    val res: DataFrame = sQLContext.sql("select provincename,cityname,count(*) from log group by provincename,cityname")

    res.show()
    //res.coalesce(1).write.partitionBy("privincename","cityname").json(outputPath)
    //加载配置文件,需要使用对应的依赖
//    val load = ConfigFactory.load()
//    val prop = new Properties()
//    prop.setProperty("user",load.getString("jdbc.user"))
//    prop.setProperty("password",load.getString("jdbc.password"))
//    res.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)
    sc.stop()

  }
}