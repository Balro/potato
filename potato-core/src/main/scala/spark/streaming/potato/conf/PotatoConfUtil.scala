package spark.streaming.potato.conf

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import scala.collection.JavaConversions.propertiesAsScalaMap

@deprecated
object PotatoConfUtil extends Logging {
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
