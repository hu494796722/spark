package spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxxxx"),
      ("cba", "xxxxxxxxx"),
      ("wnba", "xxxxxxxxx"),
      ("nba", "xxxxxxxxx"),
    ), 3)


    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    //    val partRDD: RDD[(String, String)] = rdd.partitionBy(new RangePartitioner(3,rdd,true))
    //    val partRDD: RDD[(String, String)] = rdd.partitionBy(new HashPartitioner(3))


    partRDD.saveAsTextFile("output")

    sc.stop()

  }

  /**
   * 自定义分区器
   * 1.集成 Partitioner
   * 2.重新方法
   */

  class MyPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3

    // 根据数据的key值返回数据所在的分区索引（从 0 开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }
} 