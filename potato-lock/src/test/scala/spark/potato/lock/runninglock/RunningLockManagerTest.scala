package spark.potato.lock.runninglock

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test
import spark.potato.common.service.ServiceManager
import spark.potato.lock.conf._

import scala.collection.mutable

class RunningLockManagerTest {
  @Test
  def initTest(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[8]").setAppName("test")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY, "test02:2181")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")

    val ssc = new StreamingContext(conf, Seconds(10))

    //    new RunningLockManager().serve(ssc.sparkContext)
    new ServiceManager().ssc(ssc).serve(classOf[RunningLockManagerService])
  }

  @Test
  def tryLockTest(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[8]").setAppName("test")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY, "test02:2181")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")

    val ssc = new StreamingContext(conf, Seconds(10))

    //    val lockManager = new RunningLockManagerService().serve(ssc.sparkContext)
    val lockManager = new ServiceManager().ssc(ssc).serve(classOf[RunningLockManagerService]).asInstanceOf[RunningLockManagerService]

    println(lockManager.isLocked)

    lockManager.tryLock(3, 5000)

    println(lockManager.lock.getLock)

    TimeUnit.MINUTES.sleep(10)
  }

  @Test
  def releaseTest(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[8]").setAppName("test")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY, "test02:2181")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")

    val ssc = new StreamingContext(conf, Seconds(10))

    //    val lockManager: RunningLockManagerService = new RunningLockManagerService().serve(ssc.sparkContext)
    val lockManager = new ServiceManager().ssc(ssc).serve(classOf[RunningLockManagerService]).asInstanceOf[RunningLockManagerService]

    println(lockManager.isLocked + lockManager.lock.getLock.toString())
    lockManager.tryLock(3, 5000)
    println(lockManager.isLocked + lockManager.lock.getLock.toString())
    TimeUnit.SECONDS.sleep(10)
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

    //    val lockManager = new RunningLockManagerService().serve(ssc.sparkContext)
    val lockManager = new ServiceManager().ssc(ssc).serve(classOf[RunningLockManagerService])

    lockManager.startAndStopOnJVMExit()

    ssc.queueStream(mutable.Queue(ssc.sparkContext.makeRDD(Seq(0))))
      .foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    //      ssc.awaitTermination()

    TimeUnit.SECONDS.sleep(10)
  }
}