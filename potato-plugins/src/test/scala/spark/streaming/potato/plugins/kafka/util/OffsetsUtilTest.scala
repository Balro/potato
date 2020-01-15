package spark.streaming.potato.plugins.kafka.util

import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import org.junit.Test
import spark.streaming.potato.plugins.kafka.utils.OffsetsUtil
import spark.streaming.potato.plugins.kafka.utils.OffsetsUtilImplicits._

class OffsetsUtilTest {
  val seeds: Set[BrokerEndPoint] = Set(BrokerEndPoint(-1, "test01", 9092))

  @Test
  def commitOffsetsOnZookeeperTest(): Unit = {
    OffsetsUtil.commitOffsetsOnZookeeper(seeds, "test_potato_zoo", Map(
      TopicAndPartition("test1", 0) -> 0
    ))
  }
}
