package spark.potato.template

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.potato.common.service.ServiceManager
import spark.potato.common.util.CleanUtil
import spark.potato.common.util.CleanUtil.Cleaner

/**
 * 基本模板。
 */
abstract class Template extends Logging {
  // 命令行参数。
  protected var cmdArgs = Seq.empty[String]
  // 用于注册清理方法。
  protected val cleaner = new Cleaner()
  // 用于管理附加服务。
  protected val serviceManager = new ServiceManager()

  def main(args: Array[String]): Unit

  /**
   * 业务逻辑。
   */
  def doWork(): Unit

  /**
   * 注册清理方法，清理方法按cleanInOrder调用顺序调用。
   *
   * @param name      清理方法名称。
   * @param cleanFunc 方法体。
   */
  def cleanInOrder(name: String, cleanFunc: => Unit): Unit = {
    logInfo(s"Register clean function: $name")
    cleaner.addClean(name, () => cleanFunc)
  }

  /**
   * 按须调用所有已注册清理方法。
   */
  def clean(): Unit = {
    cleaner.clean()
  }

  /**
   * 注册清理方法，清理方法在jvm关闭时调用，调用顺序随机。
   *
   * @param name      清理方法名称。
   * @param cleanFunc 方法体。
   */
  def cleanWhenShutdown(name: String, cleanFunc: => Unit): Unit = {
    logInfo(s"Register clean function at shutdown: $name")
    CleanUtil.cleanWhenShutdown(name, cleanFunc)
  }

  def createConf(): SparkConf = new SparkConf()

  def createContext(conf: SparkConf = createConf()): SparkContext = ???

  def createStreamingContext(conf: SparkConf, durMS: Long): StreamingContext = ???
}
