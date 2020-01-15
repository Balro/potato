package spark.streaming.potato.template.template

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import spark.streaming.potato.plugins.lock.RunningLockManager
import spark.streaming.potato.template.context.PotatoContextUtil
import spark.streaming.potato.plugins.lock.LockConfigKeys._

abstract class GeneralTemplate extends Logging {
  var oConf: Option[SparkConf] = None
  var oSsc: Option[StreamingContext] = None
  var oLock: Option[RunningLockManager] = None

  lazy val conf: SparkConf = oConf.get
  lazy val ssc: StreamingContext = oSsc.get
  lazy val lock: RunningLockManager = oLock.get

  def main(args: Array[String]): Unit = {
    createConf(args)
    afterConfCreated(args)
    createContext(args)
    afterContextCreated(args)

    doWork(args)

    ssc.start()
    afterStart(args)
    try {
      ssc.awaitTermination()
    } finally {
      if (oLock.isDefined) lock.release()
      afterStop(args)
    }
  }

  // 业务逻辑。
  def doWork(args: Array[String]): Unit

  def createConf(args: Array[String]): Unit = {
    logInfo("Method createConf has been called.")
    oConf = Option(new SparkConf())
  }

  def afterConfCreated(args: Array[String]): Unit = {
    logInfo("Method afterConfCreated has been called.")
  }

  def createContext(args: Array[String]): Unit = {
    logInfo("Method createContext has been called.")

    if (oConf.isEmpty)
      throw new Exception("Spark conf is not initialized.")

    oSsc = Option(PotatoContextUtil.createContext(conf))

    if (conf.getBoolean(
      POTATO_RUNNING_LOCK_ENABLE_KEY, POTATO_RUNNING_LOCK_ENABLE_DEFAULT
    )) {
      logInfo("Enable running lock and start heartbeat.")
      oLock = Option(new RunningLockManager(ssc))
      lock.startHeartbeat()
    }
  }

  def afterContextCreated(args: Array[String]): Unit = {
    logInfo("Method afterContextCreated has been called.")
  }

  def afterStart(args: Array[String]): Unit = {
    logInfo("Method afterStart has been called.")
  }

  def afterStop(args: Array[String]): Unit = {
    logInfo("Method afterStop has been called.")
  }
}
