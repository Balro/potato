package spark.streaming.potato.plugins.kafka.conf

object KafkaConfigKeys {
  val KAFKA_SOURCE_PREFIX: String = "spark.potato.source.kafka."

  val HBASE_OFFSETS_STORAGE_PREFIX: String = KAFKA_SOURCE_PREFIX + "offsets.storage.hbase."
  val HBASE_TABLE_KEY: String = HBASE_OFFSETS_STORAGE_PREFIX + "table"
  val HBASE_TABLE_DEFAULT: String = "kafka_offsets_storage"
  val HBASE_FAMILY_KEY: String = HBASE_OFFSETS_STORAGE_PREFIX + "family"
  val HBASE_FAMILY_DEFAULT: String = "partition"
  val HBASE_CONF_PREFIX: String = HBASE_OFFSETS_STORAGE_PREFIX + "conf."
}
