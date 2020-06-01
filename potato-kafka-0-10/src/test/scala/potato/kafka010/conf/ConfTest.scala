package potato.kafka010.conf

import java.util.Properties

import org.apache.spark.SparkConf
import org.junit.Test

class ConfTest {
  @Test
  def keyTest(): Unit = {
    val props = new Properties()
    props.load(this.getClass.getResourceAsStream("/template.properties"))
    System.setProperties(props)
    val conf = new SparkConf()

    val escape = Seq(
      POTATO_KAFKA_COMMON_BOOTSTRAP_SERVERS_KEY,
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY,
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY,
      POTATO_KAFKA_CONSUMER_KEY_DESERIALIZER_KEY,
      POTATO_KAFKA_CONSUMER_VALUE_DESERIALIZER_KEY,
      POTATO_KAFKA_PRODUCER_KEY_SERIALIZER_KEY,
      POTATO_KAFKA_PRODUCER_VALUE_SERIALIZER_KEY,
      POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY,
      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY,
      POTATO_KAFKA_OFFSETS_STORAGE_AUTO_UPDATE_KEY,
      POTATO_KAFKA_OFFSETS_STORAGE_UPDATE_DELAY_KEY,
      POTATO_KAFKA_OFFSETS_STORAGE_HBASE_TABLE_KEY,
      POTATO_KAFKA_OFFSETS_STORAGE_HBASE_FAMILY_KEY,
      POTATO_KAFKA_OFFSETS_STORAGE_HBASE_ZOO_QUORUM_KEY,
      POTATO_KAFKA_OFFSETS_STORAGE_HBASE_ZOO_PORT_KEY
    ).filter(!conf.contains(_))

    assert(escape.isEmpty, escape)
  }
}
