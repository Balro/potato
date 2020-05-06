package spark.potato.template.batch

import org.apache.spark.{SparkConf, SparkContext}
import spark.potato.common.service.ServiceManager

trait BatchFunction {
  protected def createConf(): SparkConf = new SparkConf()

  protected def createContext(conf: SparkConf = createConf()): SparkContext

  // 注册附加服务。
  def registerAdditionalServices(sc: SparkContext)(implicit manager: ServiceManager): SparkContext = {
    manager.sc(sc).registerServices(sc.getConf)
    sc
  }
}
