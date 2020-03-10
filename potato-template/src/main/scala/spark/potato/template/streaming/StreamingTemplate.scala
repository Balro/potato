package spark.potato.template.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import spark.potato.common.context.StreamingContextUtil
import spark.potato.common.service.ServiceManager
import spark.potato.template.GeneralTemplate
import spark.potato.template.conf._

abstract class StreamingTemplate extends GeneralTemplate with Logging {
  protected var cmdArgs: Seq[String] = _
  protected var ssc: StreamingContext = _
  protected val serviceManager = new ServiceManager()

  protected def sc: SparkContext = ssc.sparkContext

  protected def conf: SparkConf = sc.getConf

  def main(args: Array[String]): Unit = {
    cmdArgs = args
    try {
      init()
      doWork()
      ssc.start()
      afterStart()
      ssc.awaitTermination()
    } finally {
      clear()
    }
  }

  override def init(): Unit = {
    logInfo("Init method called.")
    ssc = StreamingContextUtil.createStreamingContextWithDuration(createConf())
    serviceManager.ssc(ssc)
    initAdditionalServices()
  }

  def createConf(): SparkConf = {
    new SparkConf()
  }

  def initAdditionalServices(): Unit = {
    if (conf.contains(POTATO_TEMPLATE_ADDITIONAL_SERVICES_KEY)) {
      logInfo("InitAdditionalServices method called.")
      conf.get(POTATO_TEMPLATE_ADDITIONAL_SERVICES_KEY)
        .split(",")
        .map(_.trim)
        .foreach(serviceManager.serve)
      serviceManager.start()
    }
  }

  def afterStart(): Unit = {
    logInfo("AfterStart method called.")
  }

  override def clear(): Unit = {
    logInfo("Clear method called.")
    serviceManager.stop()
  }
}
