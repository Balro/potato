package potato.kafka010.conf

import java.util.Properties

import kafka.consumer.{ConsumerConfig => SimpleConsumerConfig}
import org.apache.hadoop.hbase.HConstants
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import spark.potato.common.conf.PropertiesImplicits.mapToProperties

class PotatoKafkaConf(conf: SparkConf, kafkaProps: Map[String, String] = Map.empty) {
  private val default: Map[String, String] = Map(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName
  )

  lazy val commonProps: Map[String, String] = default ++
    Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> kafkaProps.getOrElse(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, conf.get(POTATO_KAFKA_COMMON_BOOTSTRAP_SERVERS_KEY)))
  lazy val consumerProps: Map[String, String] = commonProps ++
    conf.getAllWithPrefix(POTATO_KAFKA_CONSUMER_PREFIX).toMap ++ kafkaProps +
    (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false") // 由于kafka010直接流使用了新api，所以需要关闭自动offset提交。
  lazy val producerProps: Map[String, String] = commonProps ++
    conf.getAllWithPrefix(POTATO_KAFKA_PRODUCER_PREFIX).toMap ++ kafkaProps
  lazy val hbaseConf: Map[String, String] = conf.getAllWithPrefix(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_CONF_PREFIX).toMap

  lazy val bootstrapServers: String = commonProps(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
  lazy val groupId: String = consumerProps(ConsumerConfig.GROUP_ID_CONFIG)
  lazy val offsetResetPolicy: String = consumerProps.getOrElse(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, POTATO_KAFKA_CONSUMER_OFFSET_RESET_DEFAULT)
  lazy val keyDeserializer: String = consumerProps.getOrElse(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, POTATO_KAFKA_CONSUMER_KEY_DESERIALIZER_DEFAULT)
  lazy val valueDeserializer: String = consumerProps.getOrElse(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, POTATO_KAFKA_CONSUMER_VALUE_DESERIALIZER_DEFAULT)
  lazy val keySerializer: String = producerProps.getOrElse(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, POTATO_KAFKA_CONSUMER_KEY_DESERIALIZER_DEFAULT)
  lazy val valueSerializer: String = producerProps.getOrElse(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, POTATO_KAFKA_CONSUMER_VALUE_DESERIALIZER_DEFAULT)

  lazy val subscribeTopics: Set[String] = conf.get(POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY).split(",").map(_.trim).toSet
  lazy val offsetsStorageType: String = conf.get(POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY, POTATO_KAFKA_OFFSETS_STORAGE_TYPE_DEFAULT)
  lazy val offsetsStorageAutoUpdate: Boolean = conf.getBoolean(POTATO_KAFKA_OFFSETS_STORAGE_AUTO_UPDATE_KEY, POTATO_KAFKA_OFFSETS_STORAGE_AUTO_UPDATE_DEFAULT.toBoolean)
  lazy val offsetsStorageUpdateDelay: Long = conf.getLong(POTATO_KAFKA_OFFSETS_STORAGE_UPDATE_DELAY_KEY, POTATO_KAFKA_OFFSETS_STORAGE_UPDATE_DELAY_DEFAULT.toLong)

  lazy val offsetsStorageHBaseZooQuorum: String = hbaseConf(HConstants.ZOOKEEPER_QUORUM)
  lazy val offsetsStorageHBaseZooPort: String = hbaseConf(HConstants.ZOOKEEPER_CLIENT_PORT)
  lazy val offsetsStorageHBaseTable: String = conf.get(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_TABLE_KEY, POTATO_KAFKA_OFFSETS_STORAGE_HBASE_TABLE_DEFAULT)
  lazy val offsetsStorageHBaseFamily: String = conf.get(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_FAMILY_KEY, POTATO_KAFKA_OFFSETS_STORAGE_HBASE_FAMILY_DEFAULT)
  lazy val offsetsStorageHBaseConf: String = conf.get(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_ZOO_QUORUM_KEY)

  lazy val toSimpleConsumerConfig: SimpleConsumerConfig = {
    SimpleConsumerOffsetsUtilImplicits.mapToConsumerConfig(consumerProps)
  }

  lazy val toConsumerProperties: Properties = consumerProps

  lazy val toProducerProperties: Properties = producerProps
}
