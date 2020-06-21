package potato.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Duration, Milliseconds, StreamingContext}
import potato.spark.conf.POTATO_SPARK_STREAMING_BATCH_DURATION_MS_KEY
import potato.common.exception.ConfigNotFoundException
import potato.common.utils.JVMCleanUtil

object StreamingUtil extends Logging {
  /**
   * 获取StreamingContext的批处理时间。
   */
  def getBatchDuration(ssc: StreamingContext): Duration = {
    val sscClazz = classOf[StreamingContext]
    val graphField = sscClazz.getDeclaredField("graph")
    graphField.setAccessible(true)
    val graphClazz = Class.forName("org.apache.spark.streaming.DStreamGraph")
    val durationField = graphClazz.getDeclaredField("batchDuration")
    durationField.setAccessible(true)
    durationField.get(graphField.get(ssc)).asInstanceOf[Duration]
  }

  /**
   * 读取[[potato.spark.conf.POTATO_SPARK_STREAMING_BATCH_DURATION_MS_KEY]] 参数作为batchDuration，创建StreamingContext。
   *
   * @param conf     SparkConf
   * @param duration 手动指定duration，-1表示从配置文件读取。其他值表示覆盖配置文件。
   * @return
   */
  def createStreamingContext(conf: SparkConf, duration: Long = -1L): StreamingContext = {
    if (duration <= 0) {
      if (conf.contains(POTATO_SPARK_STREAMING_BATCH_DURATION_MS_KEY))
        new StreamingContext(conf, Milliseconds(conf.get(POTATO_SPARK_STREAMING_BATCH_DURATION_MS_KEY).toLong))
      else
        throw ConfigNotFoundException(s"Config: $POTATO_SPARK_STREAMING_BATCH_DURATION_MS_KEY not found.")
    } else {
      new StreamingContext(conf, Milliseconds(duration))
    }
  }
}
