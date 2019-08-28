package exam

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext}


object Test2 {
  def main(args: Array[String]): Unit = {
    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)


    // 读取数据
    val ds = sc.textFile("F:\\TeacherMi\\exem\\json.txt")
    val arr: Array[String] = ds.collect()

    val arr1 = arr.map(x => {

      val jsonparse = JSON.parseObject(x)

      // 判断状态是否成功
      val status: Int = jsonparse.getIntValue("status")

      if (status == 0) return ""

      // 接下来解析内部json串，判断value都不能为空
      val regecodeJson: JSONObject = jsonparse.getJSONObject("regeocode")
      if (regecodeJson == null || regecodeJson.keySet().isEmpty) return ""


      val poisArray: JSONArray = regecodeJson.getJSONArray("pois")
      if (poisArray == null || poisArray.isEmpty) return ""

      // 创建集合 保存数据
      var list: List[(String, Int)] = List[(String, Int)]()

      // 循环输出
      for (item <- poisArray.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json: JSONObject = item.asInstanceOf[JSONObject]
          val arrType: Array[String] = json.getString("type").split(";")
          arrType.foreach(x => {
            list :+= (x, 1)
          })
        }
      }
      list
    })


    println(arr1.reduce(_ ::: _).groupBy(_._1))
   // println(arr1.toBuffer)
   // println(arr1.reduce(_ ::: _))
    //println(arr1.reduce(_:::_).groupBy(_._1).mapValues(_.size).toBuffer)
  }
}
