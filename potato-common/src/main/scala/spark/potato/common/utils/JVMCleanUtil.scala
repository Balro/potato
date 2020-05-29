package spark.potato.common.utils

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging
import spark.potato.common.exception.PotatoException

import scala.collection.mutable.ArrayBuffer

/**
 * 清理工具，用于在jvm退出时调用清理方法。。
 */
object JVMCleanUtil extends Logging {
  /**
   * 注册清理方法，在jvm退出时调用。
   *
   * @note 注册后的清理方法在调用时无法保证调用顺序。
   * @param desc 方法描述。
   * @param func 清理方法体。
   */
  def cleanWhenShutdown(desc: String, func: () => Unit): Unit = {
    logInfo(s"Registered clean function when shutdown: $desc.")

    //    sys.addShutdownHook({
    //      logInfo(s"Invoke clean shutdownhook: $desc")
    //      func
    //    })

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        logInfo(s"Invoke clean shutdownhook: $desc")
        func()
      }
    }, desc))
  }

  /**
   * 使用默认的cleaner，该cleaner会在jvm退出时按注册顺序调用清理方法。
   */
  lazy val cleaner: OrderedCleaner = new OrderedCleaner()

  /**
   * 为清理方法提供有序调用，调用顺序以addClean()调用顺序为准。
   *
   * @param revokeWhenShutdown 是否在jvm关闭时进行调用，如配置为false，否则须手动执行 clean()方法。
   * @deprecated 不建议直接创建该对象。建议使用[[JVMCleanUtil.cleaner]]预置对象。
   */
  class OrderedCleaner(revokeWhenShutdown: Boolean = true) extends Logging {
    private val funcs: ArrayBuffer[(String, () => Unit)] = ArrayBuffer.empty
    private val isInvoked: AtomicBoolean = new AtomicBoolean(false)

    def addCleanFunc(name: String, cleanFunc: () => Unit): Unit = this.synchronized {
      if (isInvoked.get())
        throw new PotatoException(s"Cleaner $this has already invoked.")
      logInfo(s"Register clean function: $name")

      funcs += (name -> cleanFunc)
    }

    def doClean(): Unit = this.synchronized {
      if (isInvoked.get())
        throw new PotatoException(s"Cleaner $this has already invoked.")
      funcs.foreach { func =>
        logInfo(s"Invoke clean function: ${func._1}")
        func._2()
      }
      isInvoked.set(true)
    }

    if (revokeWhenShutdown)
      cleanWhenShutdown(s"Cleaner $this start clean procedure.", doClean)
  }

}
