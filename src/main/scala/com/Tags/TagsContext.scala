package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 4){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,dictPath,stopPath) = args
    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

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
      val userId = TagUtils.getOnUserId(row)
      //接下来通过row数据 打上 所有标签
      val adList = TagsAd.makeTags(row) //广告标签
      val appList = TagsApp.makeTags(row, broad) //软件标签
      val providList = TagsProvid.makeTags(row) //渠道标签
      val cilentList = TagsClient.makeTags(row) //设备标签
      val keywordList = TagsKeyWord.makeTags(row, broadstop) //关键字
      val loList = TagsLocation.makeTags(row) //地域标签

      (userId, adList++appList++providList++cilentList++keywordList++loList)

    })
        .reduceByKey((list1,list2) =>
          (list1:::list2)
            .groupBy(_._1)
            .mapValues(_.foldLeft(0)(_+_._2))
            .toList
        ).foreach(println)


    sc.stop()

  }
}
