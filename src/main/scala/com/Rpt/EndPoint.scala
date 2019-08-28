//package com.Rpt
//
//
//import java.sql.{Connection, PreparedStatement}
//
//import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
//
////import com.Utils.{ConnectPool, RptUtils}
//import com.utils.RptUtils
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{DataFrame, Dataset}
//
///**
//  *终端设备
//  */
//object EndPoint {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//
//    val sc = new SparkContext(conf)
//
//    //val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.parquet.compression.codec", "snappy").getOrCreate()
//
//    //val df: DataFrame = spark.read.parquet("F://out")
//
//    import spark.implicits._
//    val ds=df.map(row => {
//      //把需要的字段全部取到
//      val requestmode: Int = row.getAs[Int]("requestmode")
//      val processnode: Int = row.getAs[Int]("processnode")
//      val iseffective: Int = row.getAs[Int]("iseffective")
//      val isbilling: Int = row.getAs[Int]("isbilling")
//      val isbid: Int = row.getAs[Int]("isbid")
//      val iswin: Int = row.getAs[Int]("iswin")
//      val adordeerid: Int = row.getAs[Int]("adorderid")
//      val winprice: Double = row.getAs[Double]("winprice")
//      val adpayment: Double = row.getAs[Double]("adpayment")
//
//      //运营
//      var ispname: String = row.getAs[String]("ispname")
//      if ("未知".equals(ispname)) {
//        ispname = "其他"
//      }
//      //网络类型
//      var networkmannername = row.getAs[String]("networkmannername")
//      if("未知".equals(networkmannername)){
//        networkmannername="其他"
//      }
//      //设备类型
//      val devicetype = row.getAs[Int]("devicetype")
//      var device=""
//      if(devicetype==1){
//        device="手机"
//      }else if(devicetype==2){
//        device="平板"
//      }else{
//        device="其他"
//      }
//
//      //操作系统
//      val client = row.getAs[Int]("client")
//      var cli=""
//      if(client==1){
//        cli="android"
//      }else if(client==2){
//        cli="ios"
//      }else{
//        cli="其他"
//      }
//      val reqList: List[Double] = RptUtils.request(requestmode, processnode)
//      val clickList: List[Double] = RptUtils.click(requestmode, iseffective)
//      val adList: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adordeerid, winprice, adpayment)
//
//      val sumList = reqList ::: clickList ::: adList
//
//      (ispname, networkmannername, device, cli, sumList)
//
//    })
//    val isPnameDS: Dataset[(String, List[Double])] = ds.map(x=>(x._1,x._5))
//    val networkmannernameDS: Dataset[(String, List[Double])] = ds.map(x=>(x._2,x._5))
//    val devicetypeDS: Dataset[(String, List[Double])] = ds.map(x=>(x._3,x._5))
//    val clientDS: Dataset[(String, List[Double])] = ds.map(x=>(x._4,x._5))
//
//    val isPnameres: RDD[(String, List[Double])] = isPnameDS.rdd.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
//    val networkmannernameres: RDD[(String, List[Double])] = networkmannernameDS.rdd.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
//    val devicetyperes: RDD[(String, List[Double])] = devicetypeDS.rdd.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
//    val clientres: RDD[(String, List[Double])] = clientDS.rdd.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
//
//    isPnameres.foreachPartition(rddtodb)
//    println(isPnameres.collect().toBuffer)
//    println(networkmannernameres.collect().toBuffer)
//    println(devicetyperes.collect().toBuffer)
//    println(clientres.collect().toBuffer)
//  }
//  def rddtodb(iterator: Iterator[(String,List[Double])]): Unit ={
//
//    val conn: Connection = ConnectPool.getConnection
//
//    var ps:PreparedStatement=null
//    val sql="insert into data (ispname,requestmode,processnode,iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment) values(?,?,?,?,?,?,?,?,?,?)"
//    iterator.foreach(line=>{
//      ps=conn.prepareStatement(sql)
//      ps.setString(1,line._1.toString)
//      ps.setDouble(2,line._2(0).toDouble)
//      ps.setDouble(3,line._2(1).toDouble)
//      ps.setDouble(4,line._2(2).toDouble)
//      ps.setDouble(5,line._2(3).toDouble)
//      ps.setDouble(6,line._2(4).toDouble)
//      ps.setDouble(7,line._2(5).toDouble)
//      ps.setDouble(8,line._2(6).toDouble)
//      ps.setDouble(9,line._2(7).toDouble)
//      ps.setDouble(10,line._2(8).toDouble)
//      ps.executeUpdate()
//
//    })
//    ConnectPool.closeCon(null,ps,conn)
//
//  }
//}
