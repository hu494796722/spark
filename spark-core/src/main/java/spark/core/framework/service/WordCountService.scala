package spark.core.framework.service

import org.apache.spark.rdd.RDD
import spark.core.framework.common.TService
import spark.core.framework.dao.WordCountDao

/**
  * 服务层
  */
class WordCountService extends TService{

    private val wordCountDao = new WordCountDao()

    // 数据分析
    def dataAnalysis() = {

        val lines = wordCountDao.readFile("datas/1.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne = words.map(word=>(word,1))
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        val array: Array[(String, Int)] = wordToSum.collect()
        array
    }
}
