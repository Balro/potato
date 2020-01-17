package spark.streaming.potato.plugins.kafka

import spark.streaming.potato.common.conf.CommonConfigKeys

object KafkaConfigKeys {
  val KAFKA_SOURCE_PREFIX: String = CommonConfigKeys.POTATO_SOURCE_PREFIX + "kafka."

  val SUBSCRIBE_TOPICS_KEY: String = KAFKA_SOURCE_PREFIX + "subscribe.topics"

  val OFFSETS_STORAGE_KEY: String = KAFKA_SOURCE_PREFIX + "offsets.storage"
  val OFFSETS_AUTO_UPDATE_KEY: String = KAFKA_SOURCE_PREFIX + "offsets.auto.update"
  val OFFSETS_AUTO_UPDATE_DEFAULT: String = "true"
  val OFFSETS_AUTO_UPDATE_DELAY_KEY: String = KAFKA_SOURCE_PREFIX + "offsets.auto.update.delay"
  val OFFSETS_AUTO_UPDATE_DELAY_DEFAULT = "0"

  val HBASE_OFFSETS_STORAGE_PREFIX: String = KAFKA_SOURCE_PREFIX + "offsets.storage.hbase."
  val HBASE_TABLE_KEY: String = HBASE_OFFSETS_STORAGE_PREFIX + "table"
  val HBASE_TABLE_DEFAULT: String = "kafka_offsets_storage"
  val HBASE_FAMILY_KEY: String = HBASE_OFFSETS_STORAGE_PREFIX + "family"
  val HBASE_FAMILY_DEFAULT: String = "partition"
  val HBASE_CONF_PREFIX: String = HBASE_OFFSETS_STORAGE_PREFIX + "conf."

  val CONSUMER_CONFIG_PREFIX: String = KAFKA_SOURCE_PREFIX + "consumer."
  val CONSUMER_GROUP_ID_KEY: String = CONSUMER_CONFIG_PREFIX + "group.id"
  val CONSUMER_OFFSET_RESET_POLICY: String = CONSUMER_CONFIG_PREFIX + "auto.offset.reset"
  val CONSUMER_BOOTSTRAP_SERVERS_KEY: String = CONSUMER_CONFIG_PREFIX + "bootstrap.servers"

  val KAFKA_SINK_PREFIX: String = CommonConfigKeys.POTATO_SINK_PREFIX + "kafka."
  val PRODUCER_CONFIG_PREFIX: String = KAFKA_SINK_PREFIX + "producer."
}
