package spark.potato.template.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import spark.potato.common.conf.POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY
import spark.potato.common.exception.PotatoStreamingException
import spark.potato.common.spark.service.ServiceManager

trait StreamingFunction {
  /**
   * @param durationMs 如 durationMs <= 0 则从 SparkConf 中提取[[spark.potato.common.conf.POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY]]的值。
   *                   如果 SparkConf 中不存在响应配置，则抛出异常。
   */
  def createStreamingContext(conf: SparkConf, durationMs: Long = 0): StreamingContext = {
    new StreamingContext(conf, Milliseconds(
      if (durationMs > 0)
        durationMs
      else if (conf.contains(POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY))
        conf.get(POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY).toLong
      else
        throw PotatoStreamingException(s"Streaming duration $durationMs is invalid.")
    ))
  }

  /**
   * 注册 Streaming 附加服务。
   */
  def registerStreamingAdditionalServices(ssc: StreamingContext)(implicit manager: ServiceManager): StreamingContext = {
    manager.ssc(ssc).registerBySparkConf(ssc.sparkContext.getConf)
    ssc
  }
}
