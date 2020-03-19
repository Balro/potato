package spark.potato.common

package object conf {
  val POTATO_APP_NAME_KEY = "spark.app.name"
  val POTATO_PREFIX = "spark.potato."
  val POTATO_COMMON_PREFIX: String = POTATO_PREFIX + "common."
  // spark streaming 批次间隔key。
  val POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY: String = POTATO_COMMON_PREFIX + "streaming.batch.duration.ms"
  val POTATO_COMMON_ADDITIONAL_SERVICES_KEY: String = POTATO_COMMON_PREFIX + "additional.services"
  val POTATO_COMMON_CUSTOM_SERVICES_CLASS_KEY: String = POTATO_COMMON_PREFIX + "custom.services.class"
}
