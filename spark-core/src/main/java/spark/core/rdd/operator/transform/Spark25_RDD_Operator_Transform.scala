package spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark25_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型)

    val dataRDD1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val value: RDD[(String, Int)] = dataRDD1.sortByKey(true)

    val value1: RDD[(String, Int)] = dataRDD1.sortByKey(false)


    value.collect().foreach(println)
    println("==========================================")
    value1.collect().foreach(println)

    sc.stop()

  }
}
