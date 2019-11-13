package spark.streaming.potato.conf

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import scala.collection.JavaConversions.propertiesAsScalaMap

@deprecated("不再使用，改为使用spark-submit.sh中的 --properties-file 参数进行加载。")
object PotatoConfUtil extends Logging {
  @deprecated("不再使用，改为使用spark-submit.sh中的 --properties-file 参数进行加载。")
  def load(conf: SparkConf = new SparkConf(), path: String = "potato.properties"): SparkConf = {
    val input = PotatoConfUtil.getClass.getClassLoader.getResourceAsStream(path)
    val prop = new Properties()
    try {
      prop.load(input)
    } catch {
      case e: Exception =>
        logError("potato.properties not found.", e)
        System.exit(1)
    }
    prop.foreach(p => conf.set(p._1, p._2))
    conf
  }
}
