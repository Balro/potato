package spark.potato.common.service

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging

/**
 * service特质，用于创建附加服务。
 */
trait Service extends Logging {
  private val started = new AtomicBoolean(false)

  /**
   * 建议实现为幂等操作，有可能多次调用start方法。
   * 或者直接调用checkAndStart()。
   */
  def start(): Unit

  /**
   * 检查服务状态，如服务未启动则启动服务，如服务已启动则不做操作。
   */
  def checkAndStart(): Unit = this.synchronized {
    if (started.get()) {
      logWarning(s"Service $this already started.")
      return
    }
    start()
  }

  /**
   * 建议实现为幂等操作，有可能多次调用stop方法。
   */
  def stop(): Unit

  private val registeredStopOnJVMExit = new AtomicBoolean(false)

  /**
   * 当需要在jvm停止时同时停止服务，则调用该方法。当jvm退出时会自动调用stop()。
   */
  def stopOnJVMExit(): Unit = {
    if (registeredStopOnJVMExit.get()) {
      logInfo(s"Service ${this.getClass} already registered stop on jvm exit, no longer")
      return
    }
    logInfo(s"Service ${this.getClass} registered stop on jvm exit.")
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = stop()
    }))
  }

  /**
   * 快捷方法，启动服务同时盗用stopOnJVMExit()方法。
   *
   * @param check 是否检查服务状态，如服务已启动则不作操作。
   */
  def startAndStopOnJVMExit(check: Boolean = true): Unit = {
    if (check)
      checkAndStart()
    else
      start()
    stopOnJVMExit()
  }
}
