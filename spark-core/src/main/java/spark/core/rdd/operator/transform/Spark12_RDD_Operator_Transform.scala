package spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc: SparkContext = new SparkContext(sparkConf)

        // TODO 算子 - sortBy
        val rdd: RDD[Int] = sc.makeRDD(List(6, 2, 4, 5, 3, 1), 2)

        val newRDD: RDD[Int] = rdd.sortBy(i => i)

        newRDD.saveAsTextFile("output")

        sc.stop()


    }
}
