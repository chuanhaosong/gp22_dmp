package com.Rpt


import com.utils.RptUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 媒体分析
  */
object Media {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("目录参数不正确，退出")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    val Array(inputPath2,outputPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式 采用kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //app_dict 清洗文件
    val df1 = sc.textFile("dir/app_dict.txt")
    //parquet 数据文件
    val df2 = sQLContext.read.parquet(inputPath2)

    val video: RDD[(String, String)] = df1.map(line => {
      val x = line.split("\t")
      var id = ""
      var name = ""
      try {
        id = x(4)
        name = x(1)
      }catch {
        case e: Exception=>{
          id=""
          name=""
        }
      }
      (id, name)
    }).filter(_._1.length>0)

    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(video.collect.toMap)

    val files = df2.map(row => {
      //把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")

      var name =""
      if(appname.equals("其他" ) || appname.equals("未知")) {
        //      if(appname.equals("'其他' |'未知'") ) {
        name = broadcast.value.getOrElse(appid, null)
      }
      else{
        name = appname
      }
      val reqlist: List[Double] = RptUtils.request(requestmode,processnode)
      val clicklist: List[Double] = RptUtils.click(requestmode,iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)

      (name,reqlist++clicklist++adlist)
    })

    files.filter(_._1 != null).reduceByKey(
      (list1, list2) => {
        list1.zip(list2)
          .map(x => x._1 + x._2)
      }).foreach(println)

    sc.stop()

  }

}
