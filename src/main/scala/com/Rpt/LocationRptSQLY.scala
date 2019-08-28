package com.Rpt


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object LocationRptSQLY {

  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if (args.length != 1) {
      println("目录参数不正确，退出")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    val Array(inputPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式 采用kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val df = sQLContext.read.parquet(inputPath)

    df.registerTempTable("location")

    sQLContext.sql("select " +
      "provincename," +
      "cityname," +
      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end ) as orgin," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end ) as effect ," +
      "sum(case when requestmode=1 and processnode=3 then 1 else 0 end ) as adeffect " +
      "from location " +
      "group by provincename,cityname").registerTempTable("request")

    sQLContext.sql("select " +
      "*,adclick/adshow as clickrate " +
      "from " +
      "(select " +
      " provincename,cityname," +
      " sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as adshow, " +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as adclick " +
      "from location " +
      "group by provincename,cityname)" +
      "t ")
      .registerTempTable("click")

    sQLContext.sql("select " +
      "* ,bidsucc/bid as bidsuccrate " +
      "from " +
      "(select provincename, cityname, " +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as bid , " +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid != 0 then 1 else 0 end) as bidsucc, " +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice else 0 end)/1000 as consume, " +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment else 0 end)/1000 as cost  " +
      "from location " +
      "group by provincename,cityname)" +
      "t")
      .registerTempTable("ad")
    //
    sQLContext.sql("select r.*, a. bid,a.bidsucc,a.bidsuccrate, c.adshow,c.adclick, c.clickrate,a.consume,a.cost  " +
      "from request r " +
      "join click c on r.provincename = c.provincename and r.cityname=c.cityname " +
      "join ad a on r.provincename = a.provincename and r.cityname=a.cityname" ).show()
  }

}
