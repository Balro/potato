package spark.potato.template.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import spark.potato.common.context.StreamingContextUtil
import spark.potato.common.service.ServiceManager
import spark.potato.template.GeneralTemplate
import spark.potato.template.conf._

/**
 * 通用streaming作业模板。继承此抽象类使用。
 */
abstract class StreamingTemplate extends GeneralTemplate with Logging {
  protected var cmdArgs: Seq[String] = _
  protected var ssc: StreamingContext = _
  protected val serviceManager = new ServiceManager()

  protected def sc: SparkContext = ssc.sparkContext

  protected def conf: SparkConf = sc.getConf

  /**
   * 主流程:
   * 1.保存命令行参数。
   * 2.初始化init()，包含创建SparkConf的createConf()，注册附加服务initAdditionalServices()。
   * 3.业务逻辑代码doWork()，无默认实现。
   * 4.启动StreamingContext。
   * 5.启动后续工作afterStart()。
   * 6.进行清理clear()，默认停止附加服务管理器。
   */
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

  /**
   * 创建SparkConf的createConf()，注册附加服务initAdditionalServices()。
   */
  override def init(): Unit = {
    logInfo("Init method called.")
    ssc = StreamingContextUtil.createStreamingContextWithDuration(createConf())
    serviceManager.ssc(ssc)
    initAdditionalServices()
  }

  /**
   * 创建SparkConf，默认 new SparkConf()。
   */
  def createConf(): SparkConf = {
    new SparkConf()
  }

  /**
   * 初始化附加服务管理器，并启动。
   */
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

  /**
   * 停止附加服务管理器。
   */
  override def clear(): Unit = {
    logInfo("Clear method called.")
    serviceManager.stop()
  }
}
