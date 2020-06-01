package potato.spark.lock.singleton

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.junit.Test
import potato.spark.conf._

class SingletonLockServiceTest {
  val doNothing: Watcher = new Watcher {
    override def process(event: WatchedEvent): Unit = Unit
  }
  val zoo = new ZooKeeper("test02:2181", 30000, doNothing)

  @Test
  def outerStopTest(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
      .set(POTATO_LOCK_SINGLETON_TYPE_KEY, POTATO_LOCK_SINGLETON_TYPE_DEFAULT)
      .set(POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY, "test02:2181")
    val sc = SparkContext.getOrCreate(conf)
    val service = new ContextSingletonLockService()
    service.serve(sc)
    service.checkAndStart()
    assert(!sc.isStopped)
    TimeUnit.SECONDS.sleep(2)
    zoo.delete(s"$POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT/${sc.applicationId}.lock", -1)
    TimeUnit.SECONDS.sleep(2)
    assert(sc.isStopped)
  }
}
