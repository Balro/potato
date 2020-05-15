package spark.potato.common.service

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import spark.potato.common.threads.JVMCleanUtil

/**
 * service特质，用于创建附加服务。
 */
trait Service extends Logging {
  val serviceName: String

  private val started = new AtomicBoolean(false)

  /**
   * 建议实现为幂等操作，有可能多次调用start方法。
   * 或者直接调用checkAndStart()方法。
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
    started.set(true)
  }

  /**
   * 建议实现为幂等操作，有可能多次调用stop方法。
   * 或者直接调用checkAndStop()方法。
   */
  def stop(): Unit

  def checkAndStop(): Unit = this.synchronized {
    if (started.get()) {
      stop()
      started.set(false)
      return
    }
    logWarning(s"Service $this already stopped.")
  }

  private val registeredStopOnJVMExit = new AtomicBoolean(false)

  /**
   * 当需要在jvm停止时同时停止服务，则调用该方法。当jvm退出时会自动调用stop()。
   */
  def stopOnJVMExit(): Unit = {
    if (registeredStopOnJVMExit.get()) {
      logInfo(s"Service ${this.getClass} already registered stop on jvm exit.")
      return
    }
    logInfo(s"Service ${this.getClass} registered stop on jvm exit.")
    JVMCleanUtil.cleanWhenShutdown(s"${this.getClass}", {
      stop()
    })
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

/**
 * 只需要配置文件，不依赖spark运行的服务。
 */
trait GeneralService extends Service {
  /**
   * 初始化服务。
   */
  def serve(conf: Map[String, String]): GeneralService
}

/**
 * 依赖SparkContext的服务。
 */
trait ContextService extends Service {
  /**
   * 初始化服务。
   */
  def serve(sc: SparkContext): ContextService
}

/**
 * 依赖StreamingContext的服务。
 */
trait StreamingService extends Service {
  /**
   * 初始化服务。
   */
  def serve(ssc: StreamingContext): StreamingService
}
