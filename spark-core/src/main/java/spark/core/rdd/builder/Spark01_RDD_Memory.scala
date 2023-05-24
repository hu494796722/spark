package spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark01_RDD_Memory {

  def main(args: Array[String]): Unit = {

    // TODO 准备环境，链接

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 创建DD
    // 从内存中创建RDD，将内存中的集合的数据作为处理的数据远
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)

    // parallelize : 并行 ： 该数据集可以在集群中并行处理
    // val rdd: RDD[Int] = sc.parallelize(seq,2)
    // makeRDD方法在底层实现时其实就是调用了parallelize方法
    // val rdd: RDD[Int] = sc.makeRDD(seq, 2)
    val rdd: RDD[Int] = sc.makeRDD(seq)


    rdd.collect()
      .foreach(println)





    // TODO 关闭环境，结束
    sc.stop()

  }

}
