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
    ssc.start()
    afterStart(ssc)
    ssc.awaitTermination()
  }

  /**
   * 启动ssc后执行的操作。默认误操作。
   */
  def afterStart(ssc: StreamingContext): Unit = {}

  /**
   * 如durMs未指定，从SparkConf中提取参数值，并返回StreamingContext。
   * 创建StreamingContext会默认会使用ServiceManager注册所有配置服务。
   * 如无需注册附加服务，请不要配置spark.potato.common.additional.services或者将该参数值设置为false。
   *
   * @param durMS 批处理间隔，单位毫秒。
   */
  override def createStreamingContext(conf: SparkConf = createConf(), durMS: Long = -1): StreamingContext = {
    val dur = Milliseconds(
      if (durMS < 0) conf.getLong(POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY, -1)
      else durMS)
    val ssc = new StreamingContext(conf, dur)
    registerAdditionalServices(ssc)
  }
}
