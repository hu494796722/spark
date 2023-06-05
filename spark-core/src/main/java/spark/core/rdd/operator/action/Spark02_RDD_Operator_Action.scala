package spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // TODO - 行动算子

        // reduce
        val i: Int = rdd.reduce(_ + _)
        println(i)

        // collect : 方法会将不同分区的数据按照分区顺序采集到 Driver 端的内存中，形成数组
        val ints: Array[Int] = rdd.collect()
        println(ints.mkString(","))

        // count : 数据源中的数据个数
        val cnt: Long = rdd.count()
        println(cnt)

        // first : 获取数据源中第一个数据
        val first: Int = rdd.first()
        println(first)

        // take : 获取N个数据
        val take: Array[Int] = rdd.take(3)
        println(take.mkString(","))

        // takeOrdered : 数组排序后，取N个数据
        val rdd1 = sc.makeRDD(List(4,2,3,1))
        val ints1: Array[Int] = rdd1.takeOrdered(3)
        println(ints1.mkString(","))


        sc.stop()

    }
}
