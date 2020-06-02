package potato.kafka010.offsets

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.Test
import potato.common.conf.PropertiesImplicits.mapToProperties

class KafkaConsumerOffsetsUtilTest {
  val props: Properties = mapToProperties(Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "test02:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  ))

  @Test
  def validatedOffsetsTest(): Unit = {
    val pos = Map(new TopicPartition("test1", 0) -> 916L)
    println(KafkaConsumerOffsetsUtil.validatedOffsets(props, pos))
    println(KafkaConsumerOffsetsUtil.validatedOffsets(props, pos, reset = false))
  }

  @Test
  def gatTopicInfoTest(): Unit = {
    println(KafkaConsumerOffsetsUtil.getTopicsInfo(props, Set("test")))
  }

  @Test
  def gatLatestOffsetsTest(): Unit = {
    println(KafkaConsumerOffsetsUtil.getLatestOffsets(props, KafkaConsumerOffsetsUtil.getTopicsPartition(props, Set("test"))))
  }

  @Test
  def gatEarliestOffsetsTest(): Unit = {
    println(KafkaConsumerOffsetsUtil.getEarliestOffsets(props, KafkaConsumerOffsetsUtil.getTopicsPartition(props, Set("test"))))
  }

  @Test
  def commitOffsetsOnKafkaTest(): Unit = {
    val groupId = "test_zookeeper_zoo"
    val topic = "test"
    KafkaConsumerOffsetsUtil.commitOffsets(props, Map(
      new TopicPartition(topic, 0) -> 111,
      new TopicPartition(topic, 1) -> 222
    ), groupId)
    println(KafkaConsumerOffsetsUtil.fetchOffsets(props,
      KafkaConsumerOffsetsUtil.getTopicsPartition(props, Set(topic)), groupId))
  }
}
