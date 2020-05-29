package spark.potato.lock.singleton

import org.apache.spark.{SparkConf, SparkContext}
import spark.potato.common.spark.service.Service

abstract class SingletonLockService extends Service {
  val sc: SparkContext
  val conf: SparkConf

  /**
   * 建议实现为幂等操作，有可能多次调用start方法。
   * 或者直接调用checkAndStart()方法。
   */
  override def start(): Unit = ???

  /**
   * 建议实现为幂等操作，有可能多次调用stop方法。
   * 或者直接调用checkAndStop()方法。
   */
  override def stop(): Unit = ???
}
