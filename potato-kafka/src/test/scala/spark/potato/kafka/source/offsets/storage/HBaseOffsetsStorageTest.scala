package spark.potato.kafka.source.offsets.storage

import kafka.common.TopicAndPartition
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test
import spark.potato.kafka.KafkaConfigKeys._

class HBaseOffsetsStorageTest {
  @Test
  def saveTest(): Unit = {
    val storage = new HBaseOffsetsStorage(KAFKA_HBASE_TABLE_DEFAULT, Map(
      "hbase.zookeeper.quorum" -> "test01,test02",
      "hbase.zookeeper.property.clientPort" -> "2181"
    ))

    storage.save("hb_store_test", Map(TopicAndPartition("test", 0) -> 1))
  }

  @Test
  def loadTest(): Unit = {
    val storage = new HBaseOffsetsStorage(KAFKA_HBASE_TABLE_DEFAULT, Map(
      "hbase.zookeeper.quorum" -> "test01,test02",
      "hbase.zookeeper.property.clientPort" -> "2181"
    ))

    println(storage.load("hb_store_test", Set(
      TopicAndPartition("test", 0),
      TopicAndPartition("test", 1)
    )))

    storage.save("hb_store_test", Map(
      TopicAndPartition("test", 0) -> 123,
      TopicAndPartition("test", 1) -> 321
    ))

    println(storage.load("hb_store_test", Set(
      TopicAndPartition("test", 0),
      TopicAndPartition("test", 1)
    )))
  }

  @Test
  def hbaseTimeoutTest(): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "test01,test02")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    println("hbase.client.retries.number " + conf.get("hbase.client.retries.number"))
    conf.set("hbase.client.retries.number", "10")
    val conn = ConnectionFactory.createConnection(conf)
    println(conn.isClosed)
    val tbl: Table = conn.getTable(TableName.valueOf("test"))
    println(tbl)
    println(conf.get("hbase.client.pause"))
    println(tbl.getReadRpcTimeout)
    println(tbl.getWriteRpcTimeout)
    println(tbl.getOperationTimeout)
    val cur = System.currentTimeMillis()
    try {
      println(tbl.get(new Get(Bytes.toBytes("hello"))))
    } finally {
      println(System.currentTimeMillis() - cur)
    }
  }
}
