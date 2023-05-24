package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitionsWithIndex
        val rdd = sc.makeRDD(List(1,2,3,4), 2)
        //  算子的第一个参数为分区的索引下标，从0开始的
        //  算子的第二个参数为迭代器
        //  保留第2个分区的数据，第一个分区的数据不要了。
        // 【1，2】，【3，4】
        val mpiRDD = rdd.mapPartitionsWithIndex(
            (index,iter) => {
                if ( index == 1 ) {
                    iter
                } else {
                    Nil.iterator
                }

            }
        )
        mpiRDD.collect().foreach(println)


        sc.stop()

    }
}
