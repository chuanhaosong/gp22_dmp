package com.Rpt

import com.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 运营商名称
  */
object IspNameRpt {
  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if (args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")//设置序列化方式，比默认序列化方式性能高
    //创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //获取数据
    val df = sQLContext.read.parquet(inputPath)
    df.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  是运营商名称
      val isp = row.getAs[String]("ispname")

      //创建三个对应的方法处理九个指标
      val reqlist: List[Double] = RptUtils.request(requestmode,processnode)
      val clicklist: List[Double] = RptUtils.click(requestmode,iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective,isbilling,isbid,iswin,adorderid,WinPrice,adpayment)

      (isp,reqlist++clicklist++adlist)
    }).reduceByKey(
      (list1,list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).saveAsTextFile(outputPath)

    sc.stop()
  }
}
