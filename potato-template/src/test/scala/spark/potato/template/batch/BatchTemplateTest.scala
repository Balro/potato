package spark.potato.template.batch

import org.apache.spark.SparkConf
import org.junit.Test
import spark.potato.common.conf._
import spark.potato.common.util.ContextUtil._
import spark.potato.common.util.LocalLauncherUtil
import spark.potato.lock.conf._
import spark.potato.lock.running.ContextRunningLockService

object BatchTemplateTest extends BatchTemplate {
  override def doWork(): Unit = {
    val sc = createContext().stopWhenShutdown
    val rdd = sc.makeRDD(1 until 10)
    println(rdd.sum())
  }

  override def createConf(): SparkConf = {
    super.createConf()
      .set(POTATO_COMMON_ADDITIONAL_SERVICES_KEY,
        Seq(
          classOf[ContextRunningLockService]
        )
          .map(_.getName).mkString(","))

      // running lock
      .set(POTATO_RUNNING_LOCK_ZOOKEEPER_QUORUM_KEY, "test01:2181")
      .set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")
      .set(POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY, "90000")
      .set(POTATO_RUNNING_LOCK_TRY_INTERVAL_MS_KEY, "5000")
      .set(POTATO_RUNNING_LOCK_HEARTBEAT_INTERVAL_MS_KEY, "5000")
  }
}

class BatchTemplateTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.test(BatchTemplateTest)
  }
}
