package spark.potato.common.service

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging

/**
 * service特质，用于创建附加服务。
 */
trait Service extends Logging {
  def start(): Unit

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
}
