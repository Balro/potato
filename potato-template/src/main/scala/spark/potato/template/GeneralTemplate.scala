package spark.potato.template

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import spark.potato.common.context.PotatoContextUtil
import spark.potato.common.exception.SparkContextNotInitializedException
import spark.potato.common.service.{ServiceTrait, ServiceInfo}
import spark.potato.monitor.BacklogMonitor
import spark.potato.lock.LockConfigKeys._
import spark.potato.lock.RunningLockManager
import spark.potato.monitor.MonitorConfigKeys._

import scala.collection.mutable.ListBuffer

abstract class GeneralTemplate extends Logging {
  private var ssc: StreamingContext = _
  private val defaultServices = Seq(
    ServiceInfo(POTATO_RUNNING_LOCK_ENABLE_KEY, POTATO_RUNNING_LOCK_ENABLE_DEFAULT, classOf[RunningLockManager].getName),
    ServiceInfo(MONITOR_BACKLOG_ENABLE_KEY, MONITOR_BACKLOG_ENABLE_DEFAULT, classOf[BacklogMonitor].getName)
  )
  private val activeServices = ListBuffer.empty[ServiceTrait]

  def getConf: SparkConf = {
    if (ssc == null)
      throw SparkContextNotInitializedException()
    ssc.sparkContext.getConf
  }

  def getSsc: StreamingContext = ssc

  def main(args: Array[String]): Unit = {
    val conf = createConf(args)
    afterConfCreated(args, conf)
    ssc = createContext(args, conf)
    afterContextCreated(args)
    activeServices ++= createDefaultService() ++= createAdditionalService(args)

    try {
      startServices()
      doWork(args)
      ssc.start()
      afterStart(args)
      ssc.awaitTermination()
    } finally {
      stopServices()
      afterStop(args)
    }
  }

  // 业务逻辑。
  def doWork(args: Array[String]): Unit

  def createConf(args: Array[String]): SparkConf = {
    logInfo("Method createConf has been called.")
    new SparkConf()
  }

  def afterConfCreated(args: Array[String], conf: SparkConf): Unit = {
    logInfo("Method afterConfCreated has been called.")
  }

  def createContext(args: Array[String], conf: SparkConf): StreamingContext = {
    logInfo("Method createContext has been called.")

    if (conf == null)
      throw new Exception("Spark conf is not initialized.")

    PotatoContextUtil.createStreamingContext(conf)
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

  private def createDefaultService(): Seq[ServiceTrait] = {
    logInfo("Method registerDefaultService has been called.")
    defaultServices.filter { info =>
      getConf.getBoolean(info.key, info.default)
    }.map { info =>
      logInfo(s"Register default service ${info.clazz}")
      Class.forName(info.clazz).getConstructor(classOf[StreamingContext]).newInstance(ssc).asInstanceOf[ServiceTrait]
    }
  }

  def createAdditionalService(args: Array[String]): Seq[ServiceTrait] = {
    logInfo("Method registerAdditionalService has been called.")
    Seq.empty
  }

  private def startServices(): Unit = {
    activeServices.foreach {
      service =>
        service.start()
        logInfo(s"Service: $service started.")
    }
  }

  private def stopServices(): Unit = {
    activeServices.foreach {
      service =>
        service.stop()
        logInfo(s"Service: $service stopped.")
    }
  }

}
