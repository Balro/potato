package spark.streaming.potato.template.template

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import spark.streaming.potato.plugins.lock.LockConfigKeys._
import spark.streaming.potato.plugins.lock.RunningLockManager
import spark.streaming.potato.plugins.monitor.BacklogMonitor
import spark.streaming.potato.plugins.monitor.MonitorConfigKeys._
import spark.streaming.potato.common.context.{PotatoContextUtil, Service, ServiceInfo}

import scala.collection.mutable.ListBuffer

abstract class GeneralTemplate extends Logging {
  protected var oConf: Option[SparkConf] = None
  protected var oSsc: Option[StreamingContext] = None
  protected val additionServices: ListBuffer[Service] = ListBuffer.empty[Service]

  def conf: SparkConf = oConf.get
  def ssc: StreamingContext = oSsc.get

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
      stopAdditionServices()
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
  }

  def afterContextCreated(args: Array[String]): Unit = {
    logInfo("Method afterContextCreated has been called.")
    services.foreach { info =>
      if (conf.getBoolean(info.key, info.default))
        addService(Class.forName(info.clazz).getConstructor(classOf[StreamingContext]).newInstance(ssc).asInstanceOf[Service])
    }

    startAdditionServices()
  }

  def afterStart(args: Array[String]): Unit = {
    logInfo("Method afterStart has been called.")
  }

  def afterStop(args: Array[String]): Unit = {
    logInfo("Method afterStop has been called.")
  }

  private def addService(service: Service): Unit = {
    additionServices += service
    logInfo(s"Service: $service added to the app.")
  }

  private def startAdditionServices(): Unit = {
    additionServices.foreach { service =>
      service.start()
      logInfo(s"Service: $service started.")
    }
  }

  private def stopAdditionServices(): Unit = {
    additionServices.foreach { service =>
      service.stop()
      logInfo(s"Service: $service stopped.")
    }
  }

  private val services = Seq(
    ServiceInfo(POTATO_RUNNING_LOCK_ENABLE_KEY, POTATO_RUNNING_LOCK_ENABLE_DEFAULT, classOf[RunningLockManager].getName),
    ServiceInfo(MONITOR_BACKLOG_ENABLE_KEY, MONITOR_BACKLOG_ENABLE_DEFAULT, classOf[BacklogMonitor].getName)
  )

}
