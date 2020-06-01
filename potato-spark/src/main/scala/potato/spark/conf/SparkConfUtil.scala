package potato.spark.conf

import java.io.FileReader
import java.util.Properties

import org.apache.spark.SparkConf

import scala.collection.JavaConversions.propertiesAsScalaMap

object SparkConfUtil {
  def loadPropertiesToEnv(props: Properties): Unit = props.foreach { f =>
    System.setProperty(f._1, f._2)
  }

  def loadFileToEnv(file: String): Unit = {
    val props = new Properties()
    props.load(new FileReader(file))
    loadPropertiesToEnv(props)
  }

  class LoadableConf(conf: SparkConf) {
    def loadProperties(props: Properties): SparkConf = {
      props.foreach { f =>
        conf.set(f._1, f._2)
      }
      conf
    }

    def loadPropertyFile(file: String): SparkConf = {
      val props = new Properties()
      props.load(new FileReader(file))
      loadProperties(props)
    }
  }

  implicit def conf2Loadable(conf: SparkConf): LoadableConf = new LoadableConf(conf)
}
