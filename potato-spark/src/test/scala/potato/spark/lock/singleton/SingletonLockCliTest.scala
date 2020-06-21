package potato.spark.lock.singleton

import org.junit.Test
import potato.spark.conf._

class SingletonLockCliTest {
  @Test
  def stateTest(): Unit = {
    val args = Array(
      "--state",
      "--id", "test_app",
      "--type", "zookeeper",
      "--zoo-quorum", "test02:2181",
      "--zoo-path", s"$POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT"
    )
    SingletonLockCli.main(args)
  }

  @Test
  def cleanTest(): Unit = {
    val args = Array(
      "--clean",
      "--id", "test_app",
      "--type", "zookeeper",
      "--zoo-quorum", "test02:2181",
      "--zoo-path", s"$POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT"
    )
    SingletonLockCli.main(args)
  }
}
