package spark.potato.template.batch

import org.apache.spark.SparkConf
import org.junit.Test
import spark.potato.common.conf._
import spark.potato.common.spark.LocalLauncherUtil
import spark.potato.common.spark.SparkContextUtil._
import spark.potato.lock.conf._

object BatchTemplateTest extends BatchTemplate {
  override def doWork(): Unit = {
    val sc = createContext()
    stopOnJVMExit(sc)
    val rdd = sc.makeRDD(1 until 10)
    println(rdd.sum())
  }

  override def createConf(): SparkConf = {
    super.createConf()
      .set(POTATO_COMMON_ADDITIONAL_SERVICES_KEY, POTATO_LOCK_SINGLETON_CONTEXT_SERVICE_NAME)

      // running lock
      .set(POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY, "test01:2181")
      .set(POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY, "/potato/lock/test")
      .set(POTATO_LOCK_SINGLETON_HEARTBEAT_TIMEOUT_MS_KEY, "90000")
      .set(POTATO_LOCK_SINGLETON_TRY_INTERVAL_MS_KEY, "5000")
      .set(POTATO_LOCK_SINGLETON_HEARTBEAT_INTERVAL_MS_KEY, "5000")
  }
}

class BatchTemplateTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.localTest(BatchTemplateTest)
  }
}
