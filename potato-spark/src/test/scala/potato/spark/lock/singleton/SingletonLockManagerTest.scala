package potato.spark.lock.singleton

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}
import org.junit.Test
import potato.spark.lock.singleton.SingletonLockManagerTest.SingletonLockTest.TestLockService
import potato.spark.conf._
import potato.spark.exception.CannotGetSingletonLockException

class SingletonLockManagerTest {
  val doNothing: Watcher = new Watcher {
    override def process(event: WatchedEvent): Unit = Unit
  }
  val zoo: ZooKeeper = new ZooKeeper("test02:2181", 30000, doNothing)
  val path: String = POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT

  @Test
  def withRetryTest(): Unit = {
    val service = new TestLockService()
    val manager = new SingletonLockManager(service)
    zoo.create(s"$path/${service.sc.applicationId}.lock", Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    val start = System.currentTimeMillis()
    try {
      manager.tryLock(force = false)
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[CannotGetSingletonLockException])
        assert(System.currentTimeMillis() - start > 10000)
    }
    throw new Exception()
  }
}

object SingletonLockManagerTest {

  import potato.spark.conf._

  object SingletonLockTest {

    class TestLockService extends SingletonLockService {
      override val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        .set(POTATO_LOCK_SINGLETON_TYPE_KEY, POTATO_LOCK_SINGLETON_TYPE_DEFAULT)
        .set(POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY, "test02:2181")
        .set(POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY, POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT)
        .set(POTATO_LOCK_SINGLETON_TRY_INTERVAL_MS_KEY, "5000")
      override val sc: SparkContext = SparkContext.getOrCreate(conf)

      override val serviceName: String = "test_service"

      /**
       * 建议实现为幂等操作，有可能多次调用stop方法。
       * 或者直接调用checkAndStop()方法。
       */
      override def stop(): Unit = println("---- TestLockService stopped ----")
    }

  }

}