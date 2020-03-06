package spark.potato.lock.runninglock

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test
import spark.potato.lock.conf.LockConfigKeys._

import scala.collection.mutable

class RunningLockManagerTest {
  @Test
  def initTest(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[8]").setAppName("test")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY, "test02:2181")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")

    val ssc = new StreamingContext(conf, Seconds(10))

    new RunningLockManager().serve(ssc.sparkContext)
  }

  @Test
  def tryLockTest(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[8]").setAppName("test")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY, "test02:2181")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")

    val ssc = new StreamingContext(conf, Seconds(10))

    val lockManager = new RunningLockManager().serve(ssc.sparkContext)

    println(lockManager.isLocked)

    lockManager.tryLock(3, 5000)

    println(lockManager.isLocked)

    TimeUnit.MINUTES.sleep(10)
  }

  @Test
  def releaseTest(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[8]").setAppName("test")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY, "test02:2181")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")

    val ssc = new StreamingContext(conf, Seconds(10))

    val lockManager: RunningLockManager = new RunningLockManager().serve(ssc.sparkContext)

    println(lockManager.isLocked + lockManager.lock.getLock.toString())
    lockManager.tryLock(3, 5000)
    println(lockManager.isLocked + lockManager.lock.getLock.toString())
    lockManager.release()

    TimeUnit.MINUTES.sleep(10)
  }
}

object HeartbeatTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[8]").setAppName("test")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY, "test01:2181")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")
    conf.set(POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY, "90000")
    conf.set(POTATO_RUNNING_LOCK_TRY_INTERVAL_MS_KEY, "5000")
    conf.set(POTATO_RUNNING_LOCK_HEARTBEAT_INTERVAL_MS_KEY, "5000")

    val ssc = new StreamingContext(conf, Seconds(10))

    val lockManager = new RunningLockManager().serve(ssc.sparkContext)

    try {
      lockManager.start()

      ssc.queueStream(mutable.Queue(ssc.sparkContext.makeRDD(Seq(0))))
        .foreachRDD(rdd => rdd.foreach(println))

      ssc.start()
      ssc.awaitTermination()
    } finally {
      lockManager.stop()
    }
  }
}