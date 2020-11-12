package potato.redis

import potato.common.conf._

package object conf {
  val POTATO_REDIS_PREFIX: String = POTATO_PREFIX + "redis."
  val POTATO_REDIS_HOSTS_KEY: String = POTATO_REDIS_PREFIX + "hosts"
  val POTATO_REDIS_HOSTS_DEFAULT: String = "localhost:6379"
  val POTATO_REDIS_AUTH_KEY: String = POTATO_REDIS_PREFIX + "auth"
  val POTATO_REDIS_AUTH_DEFAULT: String = ""
  val POTATO_REDIS_TIMEOUT_KEY: String = POTATO_REDIS_PREFIX + "timeout"
  val POTATO_REDIS_TIMEOUT_DEFAULT: String = "5000"
  val POTATO_REDIS_CONFIG_KEY = POTATO_REDIS_PREFIX + "config."
  val POTATO_REDIS_CLUSTER_PREFIX = POTATO_REDIS_PREFIX + "cluster."
}
