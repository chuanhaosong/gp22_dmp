package com.Rpt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object LocationRptSQL {
  def main(args: Array[String]): Unit = {

    //判断路径是否正确
    if (args.length != 1) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")//设置序列化方式，比默认序列化方式性能高
    //创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //获取数据
    val df = sQLContext.read.parquet(inputPath)
    //注册临时表
    df.registerTempTable("logs")
    //指标统计
    val res: DataFrame = sQLContext.sql(//"select * ," +
      //"t.WinPrice/1000 as `DSP广告消费`," +
      //"t.adpayment/1000 as `DSP广告成本`" +
      //"from " +
      "select " +
      "provincename," +
      "cityname," +
      "sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as `原始请求数`," +
      "sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as `有效请求数`," +
      "sum(case when requestmode = 1 and processnode >= 3 then 1 else 0 end) as `广告请求数`," +
      "sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as `参与竞价数`," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as `竞价成功数`," +
      "sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as `展示数`," +
      "sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as `点击数`," +
      //"winprice," +
      //"adpayment," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice else 0 end)/1000 as `DSP广告消费`," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment else 0 end)/1000 as `DSP广告成本` " +
      "from logs group by provincename,cityname")
    //结果存储到MySQL
//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "123456")

    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    res.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)

    //res.show()
    sc.stop()

  }
}
