package potato.spark.sql

import org.apache.spark.sql.{DataFrame, SaveMode}
import potato.spark.sql.writer.PotatoDataSourceWriter

trait PotatoDataSource {
  def createWriter(df: DataFrame, mode: SaveMode, options: Map[String, String]): PotatoDataSourceWriter
}

object PotatoDataSource {
  val sourceMapping: Map[String, String] = Map(
    "kafka" -> "potato.kafka010.sql.KafkaDataSource",
    "redis" -> "potato.redis.sql.RedisDataSource"
  )
}
