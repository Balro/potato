package spark.potato.template.batch

import org.apache.spark.{SparkConf, SparkContext}
import spark.potato.template.Template

/**
 * 批处理模板，简单实现main方法。
 * 继承此类并实现doWork()方法来使用。
 */
abstract class BatchTemplate extends Template {
  def main(args: Array[String]): Unit = {
    cmdArgs = args
    doWork()
  }

  override def doWork(): Unit

  override def createContext(conf: SparkConf): SparkContext = {
    val sc = SparkContext.getOrCreate(conf)
    registerAdditionalServices(sc)
  }
}
