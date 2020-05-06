package spark.potato.template

import org.apache.spark.internal.Logging
import spark.potato.common.service.ServiceManager
import spark.potato.common.threads.ExitCleanUtil
import spark.potato.common.threads.ExitCleanUtil.Cleaner

/**
 * 基本模板。
 */
abstract class Template extends Logging {
  // 命令行参数。
  protected var cmdArgs = Seq.empty[String]
  // 用于注册清理方法。
  protected lazy val cleaner = new Cleaner()
  // 用于管理附加服务。
  protected implicit lazy val serviceManager: ServiceManager = new ServiceManager()

  def main(args: Array[String]): Unit

  /**
   * 业务逻辑。
   */
  protected def doWork(): Unit

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
  protected def clean(): Unit = {
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
    ExitCleanUtil.cleanWhenShutdown(name, cleanFunc)
  }
}
