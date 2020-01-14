package spark.streaming.potato.core.source.kafka.offsets.storage

import kafka.common.TopicAndPartition
import org.junit.Test

class HbaseOffsetsStorageTest {
  @Test
  def saveTest(): Unit = {
    val storage = new HbaseOffsetsStorage(HbaseOffsetsStorage.HBASE_TABLE_DEFAULT, Map(
      "hbase.zookeeper.quorum" -> "test01,test02",
      "hbase.zookeeper.property.clientPort" -> "2181"
    ))

    storage.save("hb_store_test", Map(TopicAndPartition("test", 0) -> 1))
  }

  @Test
  def loadTest(): Unit = {
    val storage = new HbaseOffsetsStorage(HbaseOffsetsStorage.HBASE_TABLE_DEFAULT, Map(
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
