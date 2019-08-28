package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object TagsContext3 {
  def main(args: Array[String]): Unit = {
    if(args.length != 5){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,dictPath,stopPath,days) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //todo 调用Hbase API
    //加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    //创建hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    //创建HbaseConnection
    val hbcomnn= ConnectionFactory.createConnection(configuration)
    val hbadmin = hbcomnn.getAdmin
    //判断表是否可用(tableExists 方法中参数要TableNamea类型，此处用TableName.valueof 方法强转)
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      //创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      //创建列簇
      val descriptor = new HColumnDescriptor("tags")
      //将列簇加入表当中
      tableDescriptor.addFamily(descriptor)
      //把列簇加入admin中
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbcomnn.close()
    }
    //创建JobConf
    val jobConf = new JobConf(configuration)
    //指定输出类型和表 （classof也是类型转换）
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)
    //读取字典文件
    val app_dict: collection.Map[String, String] = sc.textFile(dictPath).map(_.split("\t", -1))
      .filter(_.length >= 5).map(t => (t(4), t(1))).collectAsMap()
    //广播字典
    val broad = sc.broadcast(app_dict)
    //读取停用词库
    val stopword = sc.textFile(stopPath).map((_,0)).collectAsMap()
    //广播
    val broadstop = sc.broadcast(stopword)

    //读取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    val frame = df.filter(TagUtils.OnUserId)
    //过滤符合ID的数据
    val tags = frame
      //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户ID
      val userId = TagUtils.getAllUserId(row)
      //接下来通过row数据 打上 所有标签
      val adList = TagsAd.makeTags(row) //广告标签
      val appList = TagsApp.makeTags(row, broad) //软件标签
      val providList = TagsProvid.makeTags(row) //渠道标签
      val cilentList = TagsClient.makeTags(row) //设备标签
      val keywordList = TagsKeyWord.makeTags(row, broadstop) //关键字
      val loList = TagsLocation.makeTags(row) //地域标签
      val busList = TagBusiness.makeTags(row)  //商圈标签

      (userId, adList++appList++providList++cilentList++keywordList++loList++busList)

    })
      .reduceByKey((list1,list2) =>
        (list1:::list2)
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_+_._2))
          .toList)
//      ).map{
//      case(userId,userTag)=>{
//        val put = new Put(Bytes.toBytes(userId))
//        //处理下标签
//        userTag.map(t=> t._1+"," +t._2).mkString(",")
//        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
//        (new ImmutableBytesWritable(),put)
//      }
    }
      //.saveAsHadoopDataset(jobConf)

  //}
}
