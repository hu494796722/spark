package spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - glom
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

        // 【1，2】，【3，4】
        // 【2】，【4】
        // 【6】
        // 小练习：计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
        val maxRDD: RDD[Int] = rdd.glom()
          .map(
              array => {
                  array.max
              }
          )
        println(maxRDD.collect().sum)




        sc.stop()

    }
}
