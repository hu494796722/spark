package spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - distinct
        val rdd = sc.makeRDD(List(1,2,3,4,1,2,3,4))

        // distinct算子的核心处理逻辑
        // case _ => map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
        val disRDD: RDD[Int] = rdd.distinct()
        disRDD.collect().foreach(println)

        sc.stop()

    }
}
