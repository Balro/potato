package spark.potato.kafka.conf

import spark.potato.common.conf.CommonConfigKeys

object KafkaConfigKeys {
  val KAFKA_SOURCE_PREFIX: String = CommonConfigKeys.POTATO_SOURCE_PREFIX + "kafka."

  val KAFKA_SUBSCRIBE_TOPICS_KEY: String = KAFKA_SOURCE_PREFIX + "subscribe.topics"

  val KAFKA_OFFSETS_STORAGE_KEY: String = KAFKA_SOURCE_PREFIX + "offsets.storage"
  val KAFKA_OFFSETS_AUTO_UPDATE_KEY: String = KAFKA_SOURCE_PREFIX + "offsets.auto.update"
  val KAFKA_OFFSETS_AUTO_UPDATE_DEFAULT: String = "true"
  val KAFKA_OFFSETS_AUTO_UPDATE_DELAY_KEY: String = KAFKA_SOURCE_PREFIX + "offsets.auto.update.delay"
  val KAFKA_OFFSETS_AUTO_UPDATE_DELAY_DEFAULT = "0"

  val KAFKA_HBASE_OFFSETS_STORAGE_PREFIX: String = KAFKA_SOURCE_PREFIX + "offsets.storage.hbase."
  val KAFKA_HBASE_TABLE_KEY: String = KAFKA_HBASE_OFFSETS_STORAGE_PREFIX + "table"
  val KAFKA_HBASE_TABLE_DEFAULT: String = "kafka_offsets_storage"
  val KAFKA_HBASE_FAMILY_KEY: String = KAFKA_HBASE_OFFSETS_STORAGE_PREFIX + "family"
  val KAFKA_HBASE_FAMILY_DEFAULT: String = "partition"
  val KAFKA_HBASE_CONF_PREFIX: String = KAFKA_HBASE_OFFSETS_STORAGE_PREFIX + "conf."

  val KAFKA_CONSUMER_CONFIG_PREFIX: String = KAFKA_SOURCE_PREFIX + "consumer."
  val KAFKA_CONSUMER_GROUP_ID_KEY: String = KAFKA_CONSUMER_CONFIG_PREFIX + "group.id"
  val KAFKA_CONSUMER_OFFSET_RESET_POLICY: String = KAFKA_CONSUMER_CONFIG_PREFIX + "auto.offset.reset"
  val KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY: String = KAFKA_CONSUMER_CONFIG_PREFIX + "bootstrap.servers"

  val KAFKA_SINK_PREFIX: String = CommonConfigKeys.POTATO_SINK_PREFIX + "kafka."
  val KAFKA_PRODUCER_CONFIG_PREFIX: String = KAFKA_SINK_PREFIX + "producer."
}
