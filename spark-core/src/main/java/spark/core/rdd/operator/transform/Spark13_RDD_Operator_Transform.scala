package spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)



        // TODO 算子 - 双Value类型

        // 交集，并集和差集要求两个数据源数据类型保持一致
        // 拉链操作两个数据源的类型可以不一致

        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
        val rdd999 = sc.makeRDD(List("3","4","5","6"))

        // 交集 : 【3，4】
        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
//        val rdd8: Any = rdd1.intersection(rdd999)
        println(rdd3.collect().mkString(","))

        // 并集 : 【1，2，3，4，3，4，5，6】
        val rdd4: RDD[Int] = rdd1.union(rdd2)
        println(rdd4.collect().mkString(","))

        // 差集 : 【1，2】
        // 差集 : 【5，6】
        val rdd5: RDD[Int] = rdd1.subtract(rdd2)
        val rdd6: RDD[Int] = rdd2.subtract(rdd1)
        println(rdd5.collect().mkString(","))
        println(rdd6.collect().mkString(","))

        // 拉链 : 【1-3，2-4，3-5，4-6】
        val rdd7: RDD[(Int, Int)] = rdd1.zip(rdd2)
        val rdd8: RDD[(Int, String)] = rdd1.zip(rdd999)
        println(rdd7.collect().mkString(","))
        println(rdd8.collect().mkString(","))

        println("=============================")

        // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
        // 两个数据源要求分区数量要保持一致
        // Can only zip RDDs with same number of elements in each partition
        // 两个数据源要求分区中数据数量保持一致
        val rdd9 = sc.makeRDD(List(1,2,3,4,5,6),2)
        val rdd10 = sc.makeRDD(List(3,4,5,6),2)

        val rdd11: RDD[(Int, Int)] = rdd9.zip(rdd10)
        println(rdd6.collect().mkString(","))

        sc.stop()
    }
}
