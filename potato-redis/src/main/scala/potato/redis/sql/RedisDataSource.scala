package potato.redis.sql

import org.apache.spark.sql.{DataFrame, SaveMode}
import potato.redis.sql.writer.RedisKVWriter
import potato.spark.sql.PotatoDataSource
import potato.spark.sql.writer.PotatoDataSourceWriter
import potato.redis.conf._

class RedisDataSource extends PotatoDataSource {
  override def createWriter(df: DataFrame, mode: SaveMode, options: Map[String, String]): PotatoDataSourceWriter = {
    new RedisKVWriter(df,
      options.getOrElse(POTATO_REDIS_HOSTS_KEY.substring(POTATO_REDIS_PREFIX.length), POTATO_REDIS_HOSTS_DEFAULT).split(",").toSet,
      options.getOrElse(POTATO_REDIS_TIMEOUT_KEY.substring(POTATO_REDIS_PREFIX.length), POTATO_REDIS_TIMEOUT_DEFAULT).toInt,
      options.getOrElse(POTATO_REDIS_AUTH_KEY.substring(POTATO_REDIS_PREFIX.length), POTATO_REDIS_AUTH_DEFAULT)
    )
  }
}
