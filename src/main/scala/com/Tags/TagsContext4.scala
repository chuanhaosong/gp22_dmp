package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object TagsContext4 {
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
    val broaddict = sc.broadcast(app_dict)
    //读取停用词库
    val stopword = sc.textFile(stopPath).map((_,0)).collectAsMap()
    //广播
    val broadstop = sc.broadcast(stopword)

    //读取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    val frame = df.filter(TagUtils.OnUserId)

    //过滤符合ID的数据
    val baseRDD = frame.map(row => {
      //接下来所有的标签都在内部实现
      val userList = TagUtils.getAllUserId(row)
      (userList, row)
    })

    //构建点集合
    val vertiesRDD = baseRDD.flatMap(tp => {
      val row = tp._2
      //所有标签
      val adList = TagsAd.makeTags(row)
      val appList = TagsApp.makeTags(row, broaddict)
      val keywordList = TagsKeyWord.makeTags(row, broadstop)
      val dvList = TagsClient.makeTags(row)
      val loactionList = TagsLocation.makeTags(row)
      val business = TagBusiness.makeTags(row)
      val AllTag = adList ++ appList ++ keywordList ++ dvList ++ loactionList ++ business

      //保证其中一个点携带着所有标签，同时也保留所有userID
      val VD = tp._1.map((_, 0)) ++ AllTag

      //处理所有的点集合
      tp._1.map(uId => {
        //保证一个点的携带标签 （uid,vd）,(uid,list()),(uid,list())
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })


    //构建边的集合
    val edges = baseRDD.flatMap(tp => {
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })
    edges
    //构建图
    val graph = Graph(vertiesRDD,edges)
    //取出顶点，使用的是图计算中的连通图算法
    val vertices = graph.connectedComponents().vertices
    //处理所有的标签
    vertices.join(vertiesRDD).map{
      case (uId,(conId,tagsAll)) => (conId,tagsAll)
    }.reduceByKey((list1,list2) =>{
      //聚合所有标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .take(20).foreach(println)

    sc.stop()
  }
}
