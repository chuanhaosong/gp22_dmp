package exam

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {

    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)


    // 读取数据
    val ds = sc.textFile("F:\\TeacherMi\\exem\\json.txt")
    val arr: Array[String] = ds.collect()
    val array: Array[List[(String, Int)]] = arr.map(string => {

      //      val typeList: List[(String, Int)] = TypeTag.makeTags(string)
      val jsonparse = JSON.parseObject(string)
      val status: Int = jsonparse.getIntValue("status")
      if (status == 0) return ""
      val regeocode = jsonparse.getJSONObject("regeocode")

      if (regeocode == null || regeocode.keySet().isEmpty) return ""

      val pois = regeocode.getJSONArray("pois")

      if (pois == null || pois.isEmpty) return ""
      var list = List[(String, Int)]()
      for (item <- pois.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          //          println(json.getString("businessarea").toBuffer)
          if (!json.getString("businessarea").isEmpty) {

            list :+= (json.getString("businessarea"), 1)
          }
          //          println(json.getString("businessarea"))
        }
      }

      list
    })
    println(array.reduce(_:::_).groupBy(_._1).mapValues(_.size).toBuffer)
  }
}
