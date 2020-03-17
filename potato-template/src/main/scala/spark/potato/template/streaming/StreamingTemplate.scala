package spark.potato.template.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import spark.potato.common.conf._
import spark.potato.template.Template

/**
 * 流处理模板。
 */
abstract class StreamingTemplate extends Template {
  override def main(args: Array[String]): Unit = {
    cmdArgs = args
    doWork()
  }

  /**
   * 请在doWork方法或者业务方法中调用此方法以启动ssc。
   */
  def start(ssc: StreamingContext): Unit = {
    beforeStart(ssc)
    ssc.start()
    afterStart(ssc)
    ssc.awaitTermination()
  }

  /**
   * 启动ssc前执行的操作。默认用来启动附加服务。
   */
  def beforeStart(ssc: StreamingContext): Unit = {
    serviceManager.ssc(ssc).registerAdditionalServices(ssc.sparkContext.getConf)
  }

  /**
   * 启动ssc后执行的操作。默认误操作。
   */
  def afterStart(ssc: StreamingContext): Unit = {}

  /**
   * 如durMs未指定，从SparkConf中提取批处理间隔，并返回StreamingContext。
   *
   * @param durMS 批处理间隔，单位毫秒。
   */
  override def createStreamingContext(conf: SparkConf = createConf(), durMS: Long = -1): StreamingContext = {
    val dur = Milliseconds(
      if (durMS < 0) conf.getLong(POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY, -1)
      else durMS)
    new StreamingContext(conf, dur)
  }
}
