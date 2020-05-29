package spark.potato.template

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import spark.potato.common.spark.service.ServiceManager
import spark.potato.common.threads.JVMCleanUtil.Cleaner
import spark.potato.common.utils.JVMCleanUtil

/**
 * 基本模板。
 */
abstract class PotatoTemplate extends Logging {
  // 用于注册清理方法。
  lazy val cleaner = new Cleaner()
  // 用于管理附加服务。
  implicit lazy val serviceManager: ServiceManager = new ServiceManager()

  def main(args: Array[String]): Unit

  /**
   * 按须调用所有已注册清理方法。
   */
  def clean(): Unit = cleaner.doClean()

  /**
   * 注册清理方法，清理方法在jvm关闭时调用，不保证调用顺序。
   *
   * @param name      清理方法名称。
   * @param cleanFunc 方法体。
   */
  def registerShutdownCleaner(name: String, cleanFunc: => Unit): Unit = {
    logInfo(s"Register clean function at shutdown: $name")
    JVMCleanUtil.cleanWhenShutdown(name, cleanFunc)
  }

  /**
   * 注册清理方法，清理方法按cleanInOrder调用顺序调用。
   *
   * @param name      清理方法名称。
   * @param cleanFunc 方法体。
   */
  def registerShutdownInOrderCleaner(name: String, cleanFunc: => Unit): Unit = {
    logInfo(s"Register clean function: $name")
    cleaner.addCleanFunc(name, () => cleanFunc)
  }

  /**
   * 注册 SparkContext 附加服务。
   */
  def registerScAdditionalServices(sc: SparkContext)(implicit manager: ServiceManager): SparkContext = {
    manager.sc(sc).registerBySparkConf(sc.getConf)
    sc
  }

  /**
   * 默认的 SparkConf .
   */
  lazy val defaultSparkConf: SparkConf = new SparkConf()

  /**
   * 使用默认的 SparkConf 创建 SparkContext.
   */
  lazy val defaultSparkContext: SparkContext = {
    val sc = SparkContext.getOrCreate(defaultSparkConf)
    registerScAdditionalServices(sc)
    sc
  }


}
