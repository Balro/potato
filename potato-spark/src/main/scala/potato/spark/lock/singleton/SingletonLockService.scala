package potato.spark.lock.singleton

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import potato.spark.service._

abstract class SingletonLockService extends Service {
  protected var manager: SingletonLockManager = _

  def sc: SparkContext

  def conf: SparkConf

  /**
   * 建议实现为幂等操作，有可能多次调用start方法。
   * 或者直接调用checkAndStart()方法。
   */
  override def start(): Unit = manager.tryLock()
}

/**
 * 区分StreamingContext和SparkContext，避免停止了SparkContext而未停止StreamingContext导致报错。
 */
class StreamingSingletonLockService extends SingletonLockService with StreamingService with Logging {
  override val serviceName: String = "StreamingSingletonLock"

  private var ssc: StreamingContext = _

  override def sc: SparkContext = ssc.sparkContext

  override def conf: SparkConf = sc.getConf

  override def stop(): Unit = {
    ssc.stop()
    manager.release()
  }

  /**
   * 初始化服务。
   */
  override def serve(ssc: StreamingContext): StreamingService = {
    this.ssc = ssc
    manager = new SingletonLockManager(this)
    this
  }
}

/**
 * 使用于SparkContext，不可用于StreamingContext，否则在yarn模式下降导致StreamingContext报错而意外重启。
 */
class ContextSingletonLockService extends SingletonLockService with ContextService with Logging {
  override val serviceName: String = "ContextSingletonLock"

  private var _sc: SparkContext = _

  override def sc: SparkContext = _sc

  override def conf: SparkConf = sc.getConf

  override def stop(): Unit = {
    sc.stop()
    manager.release()
  }

  /**
   * 初始化服务。
   */
  override def serve(sc: SparkContext): ContextSingletonLockService = {
    this._sc = sc
    manager = new SingletonLockManager(this)
    this
  }
}
