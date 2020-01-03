package spark.streaming.potato.conf

object PotatoConfKeys {
  val POTATO_PREFIX = "spark.potato."
  val POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY: String = POTATO_PREFIX + "streaming.slide.duration.seconds"

  val POTATO_SOURCE_PREFIX: String = POTATO_PREFIX + "source."
}

case class ConfigNotFoundException(msg: String = null, throwable: Throwable = null) extends Exception(msg, throwable)
