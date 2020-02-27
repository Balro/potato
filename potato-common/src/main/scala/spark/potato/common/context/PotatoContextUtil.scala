package spark.potato.common.context

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.potato.common.exception.ConfigNotFoundException
import spark.potato.common.conf.CommonConfigKeys.POTATO_STREAMING_BATCH_DURATION_SECONDS_KEY

object PotatoContextUtil {
  /**
   * 读取conf中的 POTATO_STREAMING_BATCH_DURATION_SECONDS_KEY 参数作为batchDuration，创建StreamingContext。
   *
   * @param conf     SparkConf
   * @param duration 手动指定duration，-1表示从配置文件读取。其他值表示覆盖配置文件。
   * @return
   */
  def createStreamingContextWithDuration(conf: SparkConf, duration: Long = -1L): StreamingContext = {
    if (duration <= 0) {
      if (conf.contains(POTATO_STREAMING_BATCH_DURATION_SECONDS_KEY))
        new StreamingContext(conf, Seconds(conf.get(POTATO_STREAMING_BATCH_DURATION_SECONDS_KEY).toLong))
      else
        throw ConfigNotFoundException(s"Config: $POTATO_STREAMING_BATCH_DURATION_SECONDS_KEY not found.")
    } else {
      new StreamingContext(conf, Seconds(duration))
    }
  }
}
