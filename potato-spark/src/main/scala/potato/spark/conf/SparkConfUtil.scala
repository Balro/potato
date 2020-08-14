package potato.spark.conf

import java.io.FileReader
import java.util.Properties

import org.apache.spark.SparkConf

import scala.collection.JavaConversions.propertiesAsScalaMap

/**
 * SparkConf工具类。用于加载配置文件。
 */
object SparkConfUtil {
  def loadPropertiesToEnv(props: Properties): Unit = props.foreach { f =>
    System.setProperty(f._1, f._2)
  }

  def loadFileToEnv(file: String): Unit = {
    val props = new Properties()
    props.load(new FileReader(file))
    loadPropertiesToEnv(props)
  }

  def loadProperties(conf: SparkConf, props: Properties): SparkConf = {
    props.foreach { f =>
      conf.set(f._1, f._2)
    }
    conf
  }

  def loadPropertyFile(conf: SparkConf, file: String): SparkConf = {
    val props = new Properties()
    props.load(new FileReader(file))
    loadProperties(conf, props)
  }

  class LoadableConf(conf: SparkConf) {
    def load(props: Properties): SparkConf = {
      loadProperties(conf, props)
    }

    def load(file: String): SparkConf = {
      loadPropertyFile(conf, file)
    }
  }

  implicit def conf2Loadable(conf: SparkConf): LoadableConf = new LoadableConf(conf)
}
