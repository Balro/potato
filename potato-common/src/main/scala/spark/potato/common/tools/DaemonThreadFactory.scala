package spark.potato.common.tools

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

/**
 * 修改自 java.util.concurrent.Executors.DefaultThreadFactory。
 * 用于创建守护线程。
 * 避免在main函数非正常退出，又没有触发service等stop函数时，导致application不退出的bug。
 *
 * @deprecated "建议使用com.google.common.util.concurrent.ThreadFactoryBuilder。"
 */
@Deprecated
object DaemonThreadFactory extends ThreadFactory {
  private val poolNumber = new AtomicInteger(1)
  private var group = null.asInstanceOf[ThreadGroup]
  private val threadNumber = new AtomicInteger(1)
  private var namePrefix = null.asInstanceOf[String]

  val s: SecurityManager = System.getSecurityManager
  group = if (s != null) s.getThreadGroup
  else Thread.currentThread.getThreadGroup
  namePrefix = "pool-" + poolNumber.getAndIncrement + "-thread-"

  override def newThread(r: Runnable): Thread = {
    val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement, 0)
    // 修改此处。
    // 原代码: if (t.isDaemon) t.setDaemon(false)
    if (!t.isDaemon) t.setDaemon(true)
    if (t.getPriority != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY)
    t
  }
}
