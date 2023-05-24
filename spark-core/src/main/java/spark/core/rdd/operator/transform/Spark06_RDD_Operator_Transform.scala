package spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

        // groupBy 会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
        // 相中的key值的数据会防止在一个组中
        val groupRDD: Unit = rdd.groupBy(_ % 2).collect().foreach(println)
        sc.stop()

    }
}
