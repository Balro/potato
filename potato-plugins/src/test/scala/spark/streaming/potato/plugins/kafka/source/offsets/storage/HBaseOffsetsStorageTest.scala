package spark.streaming.potato.plugins.kafka.source.offsets.storage

import kafka.common.TopicAndPartition
import org.junit.Test
import spark.streaming.potato.plugins.kafka.KafkaConfigKeys._

class HBaseOffsetsStorageTest {
  @Test
  def saveTest(): Unit = {
    val storage = new HBaseOffsetsStorage(HBASE_TABLE_DEFAULT, Map(
      "hbase.zookeeper.quorum" -> "test01,test02",
      "hbase.zookeeper.property.clientPort" -> "2181"
    ))

    storage.save("hb_store_test", Map(TopicAndPartition("test", 0) -> 1))
  }

  @Test
  def loadTest(): Unit = {
    val storage = new HBaseOffsetsStorage(HBASE_TABLE_DEFAULT, Map(
      "hbase.zookeeper.quorum" -> "test01,test02",
      "hbase.zookeeper.property.clientPort" -> "2181"
    ))

    println(storage.load("hb_store_test", Set(
      TopicAndPartition("test", 0),
      TopicAndPartition("test", 1)
    )))

    storage.save("hb_store_test",Map(
      TopicAndPartition("test",0)->123,
      TopicAndPartition("test",1)->321
    ))

    println(storage.load("hb_store_test", Set(
      TopicAndPartition("test", 0),
      TopicAndPartition("test", 1)
    )))
  }
}
