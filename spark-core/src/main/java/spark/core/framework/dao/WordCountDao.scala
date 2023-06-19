package spark.core.framework.dao

import spark.core.framework.common.TDao
import spark.core.framework.util.EnvUtil

// 持久层
class WordCountDao extends TDao{
  def readFile(path:String) = {
    EnvUtil.take().textFile(path)
  }
}
