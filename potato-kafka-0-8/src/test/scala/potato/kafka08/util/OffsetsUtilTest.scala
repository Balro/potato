package potato.kafka08.util

import kafka.common.TopicAndPartition
import org.junit.Test
import spark.potato.kafka.utils.OffsetsUtil
import spark.potato.kafka.utils.OffsetsUtilImplicits._

class OffsetsUtilTest {
  val seeds: Map[String, Int] = Map("test01" -> 9092)

  @Test
  def gatMetadataTest(): Unit = {
    println(OffsetsUtil.getMetadata(seeds, Set("test")))
  }

  @Test
  def gatTopicAndPartitionsTest(): Unit = {
    println(OffsetsUtil.getTopicAndPartitions(seeds, Set("test")))
  }

  @Test
  def gatLatestOffsetsTest(): Unit = {
    println(OffsetsUtil.getLatestOffsets(seeds, OffsetsUtil.getTopicAndPartitions(seeds, Set("test"))))
  }

  @Test
  def commitOffsetsOnZookeeperTest(): Unit = {
    val groupId = "test_zookeeper_zoo"
    val topic = "test"
    OffsetsUtil.commitOffsetsOnZookeeper(seeds, groupId, Map(
      TopicAndPartition(topic, 0) -> 123,
      TopicAndPartition(topic, 1) -> 321
    ))
    println(OffsetsUtil.fetchOffsetsOnZookeeper(seeds, groupId,
      OffsetsUtil.getTopicAndPartitions(seeds, Set(topic))))
  }

  @Test
  def commitOffsetsOnKafkaTest(): Unit = {
    val groupId = "test_zookeeper_zoo"
    val topic = "test"
    OffsetsUtil.commitOffsetsOnKafka(seeds, groupId, Map(
      TopicAndPartition(topic, 0) -> 111,
      TopicAndPartition(topic, 1) -> 222
    ))
    println(OffsetsUtil.fetchOffsetsOnKafka(seeds, groupId,
      OffsetsUtil.getTopicAndPartitions(seeds, Set(topic))))
  }
}
