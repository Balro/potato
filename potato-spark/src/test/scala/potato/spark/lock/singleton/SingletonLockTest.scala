package potato.spark.lock.singleton

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}
import org.junit.Test
import potato.spark.lock.singleton.SingletonLockTest.TestLockService
import potato.spark.conf._

class SingletonLockTest {
  val doNothing: Watcher = new Watcher {
    override def process(event: WatchedEvent): Unit = Unit
  }
  val zoo = new ZooKeeper("test02:2181", 30000, doNothing)

  val path: String = POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT

  @Test
  def tryLockTest(): Unit = {
    val service = new TestLockService()
    val lock: SingletonLock = new ZookeeperSingletonLock(service, "test02:2181", 30000, path, "test")
    lock.tryLock("test msg")

    assert(new String(zoo.getData(path + "/test.lock", doNothing, new Stat())) == "test msg")
    zoo.delete(path + "/test.lock", -1)
  }

  @Test
  def deleteLockTest(): Unit = {
    val service = new TestLockService()
    val lock: SingletonLock = new ZookeeperSingletonLock(service, "test02:2181", 30000, path, "test")
    lock.tryLock("test msg")

    TimeUnit.SECONDS.sleep(5)
    zoo.delete(path + "/test.lock", -1)
    assert(!lock.getMsg._1)
  }

  @Test
  def releaseLockTest(): Unit = {
    val service = new TestLockService()
    val lock: SingletonLock = new ZookeeperSingletonLock(service, "test02:2181", 30000, path, "test")
    lock.tryLock("test msg")

    TimeUnit.SECONDS.sleep(5)
    lock.release()
    assert(zoo.exists(path + "/test.lock", false) == null)
  }

  @Test
  def cannotLockTest(): Unit = {
    val service = new TestLockService()
    val lock: SingletonLock = new ZookeeperSingletonLock(service, "test02:2181", 30000, path, "test")
    zoo.create(path + "/test.lock", Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    assert(!lock.tryLock("test msg"))
  }

  @Test
  def cleanLockTest(): Unit = {
    val service = new TestLockService()
    val lock: SingletonLock = new ZookeeperSingletonLock(service, "test02:2181", 30000, path, "test")
    lock.tryLock("test msg")
    assert(zoo.exists(path + "/test.lock", false) != null)
    lock.clean()
    assert(zoo.exists(path + "/test.lock", false) == null)
  }
}

object SingletonLockTest {

  class TestLockService extends SingletonLockService {
    override val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    override val sc: SparkContext = SparkContext.getOrCreate(conf)

    override val serviceName: String = "test_service"

    /**
     * 建议实现为幂等操作，有可能多次调用stop方法。
     * 或者直接调用checkAndStop()方法。
     */
    override def stop(): Unit = println("---- TestLockService stopped ----")
  }
}