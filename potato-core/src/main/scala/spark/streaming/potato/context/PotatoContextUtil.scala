package spark.streaming.potato.context

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.streaming.potato.conf.PotatoConfKeys
import spark.streaming.potato.exception.ConfigNotFoundException

object PotatoContextUtil {
  def createContext(conf: SparkConf): StreamingContext = {
    if (conf.contains(PotatoConfKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY))
      new StreamingContext(conf, Seconds(conf.getLong(PotatoConfKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY, -1L)))
    else
      throw ConfigNotFoundException(s"Config: ${PotatoConfKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY} not found.")
  }
}
