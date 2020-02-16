package spark.streaming.potato.common.context

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.streaming.potato.common.conf.CommonConfigKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY
import spark.streaming.potato.common.exception.ConfigNotFoundException

object PotatoContextUtil {
  def createContext(conf: SparkConf): StreamingContext = {
    if (conf.contains(POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY))
      new StreamingContext(conf, Seconds(conf.getLong(POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY, -1L)))
    else
      throw ConfigNotFoundException(s"Config: $POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY not found.")
  }
}
