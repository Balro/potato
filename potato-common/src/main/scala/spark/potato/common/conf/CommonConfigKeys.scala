package spark.potato.common.conf

object CommonConfigKeys {
  val POTATO_PREFIX = "spark.potato."
  val POTATO_COMMON_PREFIX: String = POTATO_PREFIX + "common."
  // spark streaming 批次间隔key。
  val POTATO_STREAMING_BATCH_DURATION_SECONDS_KEY: String = POTATO_PREFIX + "streaming.batch.duration.seconds"
}
