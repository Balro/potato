package spark.streaming.potato.conf

object ConfigKeys {
  val POTATO_PREFIX = "spark.potato."
  val POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY: String = POTATO_PREFIX + "streaming.slide.duration.seconds"
  val POTATO_STREAMING_SLIDE_DURATION_SECONDS_DEFAULT = 60L
}
