package spark.streaming.potato.template

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import spark.streaming.potato.context.PotatoContextUtil

/**
  * 执行顺序: initConf -> initContext -> process -> beforeStart -> ssc.start -> afterStart -> ssc.awaitTermination -> afterStop
  */
trait SparkStreamingTemplate extends Logging {
  lazy val conf: SparkConf = new SparkConf()
  lazy val ssc: StreamingContext = PotatoContextUtil.makeContext(conf)

  def main(args: Array[String]): Unit = {
    initConf(args)
    initContext(args)
    doWork(args)
    beforeStart(args)
    ssc.start()
    afterStart(args)
    ssc.awaitTermination()
    afterStop(args)
  }

  // 业务逻辑。
  def doWork(args: Array[String]): Unit

  def initConf(args: Array[String]): Unit = {
    logInfo("Method initConf has been called.")
  }

  def initContext(args: Array[String]): Unit = {
    logInfo("Method initContext has been called.")
  }

  def beforeStart(args: Array[String]): Unit = {
    logInfo("Method beforeStart has been called.")
  }

  def afterStart(args: Array[String]): Unit = {
    logInfo("Method afterStart has been called.")
  }

  def afterStop(args: Array[String]): Unit = {
    logInfo("Method afterStop has been called.")
  }
}
