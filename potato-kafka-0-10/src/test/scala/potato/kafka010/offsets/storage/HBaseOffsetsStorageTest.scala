package potato.kafka010.offsets.storage

import java.util.concurrent.TimeUnit

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import potato.kafka010.conf._

class HBaseOffsetsStorageTest {
  @Test
  def saveTest(): Unit = {
    val storage = new HBaseOffsetsStorage(
      POTATO_KAFKA_OFFSETS_STORAGE_HBASE_TABLE_DEFAULT,
      POTATO_KAFKA_OFFSETS_STORAGE_HBASE_FAMILY_DEFAULT,
      Map(
        HConstants.ZOOKEEPER_QUORUM -> "test01,test02",
        HConstants.ZOOKEEPER_CLIENT_PORT -> "2181"
      ))

    storage.save("hb_store_test", Map(new TopicPartition("test", 0) -> 1))
  }

  @Test
  def loadTest(): Unit = {
    val storage = new HBaseOffsetsStorage(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_TABLE_DEFAULT,
      POTATO_KAFKA_OFFSETS_STORAGE_HBASE_FAMILY_DEFAULT,
      Map(
        "hbase.zookeeper.quorum" -> "test01,test02",
        "hbase.zookeeper.property.clientPort" -> "2181"
      ))

    storage.save("hb_store_test", Map(
      new TopicPartition("test", 0) -> 111,
      new TopicPartition("test", 1) -> 322
    ))
    println(storage.load("hb_store_test", Set(
      new TopicPartition("test", 0),
      new TopicPartition("test", 1)
    )))

    storage.save("hb_store_test", Map(
      new TopicPartition("test", 0) -> 123,
      new TopicPartition("test", 1) -> 321
    ))
    println(storage.load("hb_store_test", Set(
      new TopicPartition("test", 0),
      new TopicPartition("test", 1)
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
    println(tbl.getReadRpcTimeout(TimeUnit.MILLISECONDS))
    println(tbl.getWriteRpcTimeout(TimeUnit.MILLISECONDS))
    println(tbl.getOperationTimeout(TimeUnit.MILLISECONDS))
    val cur = System.currentTimeMillis()
    try {
      println(tbl.get(new Get(Bytes.toBytes("hello"))))
    } finally {
      println(System.currentTimeMillis() - cur)
    }
  }
}
