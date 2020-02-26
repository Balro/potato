package spark.potato.common.conf

object CommonConfigKeys {
  val POTATO_PREFIX = "spark.potato."
  val POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY: String = POTATO_PREFIX + "streaming.slide.duration.seconds"
  val POTATO_SOURCE_PREFIX: String = POTATO_PREFIX + "source."
  val POTATO_SINK_PREFIX: String = POTATO_PREFIX + "sink."
}