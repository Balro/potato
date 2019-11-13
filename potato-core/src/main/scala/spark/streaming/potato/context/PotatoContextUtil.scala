package spark.streaming.potato.context

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.streaming.potato.conf.ConfigKeys

object PotatoContextUtil {
  def makeContext(conf: SparkConf): StreamingContext = {
    new StreamingContext(conf, Seconds(conf.getLong(ConfigKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY, ConfigKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_DEFAULT)))
  }
}
