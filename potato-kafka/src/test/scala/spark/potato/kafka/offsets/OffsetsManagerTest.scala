package spark.potato.kafka.offsets

import java.util.concurrent.TimeUnit

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.OffsetRange
import org.junit.Test
import spark.potato.kafka.offsets.manager.{OffsetsManager, OffsetsManagerConf}
import spark.potato.kafka.conf._

class OffsetsManagerTest {
  @Test
  def gatStartOffsetsTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_KEY -> "zookeeper",
      POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )
    val conf = new OffsetsManagerConf(mconf)

    val manager = new OffsetsManager(conf)

    println(manager.committedOffsets())
  }


  @Test
  def cacheOffsetsTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_KEY -> "zookeeper",
      POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )
    val conf = new OffsetsManagerConf(mconf)

    val manager = new OffsetsManager(conf)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test1", 0), 0, 1)))
    TimeUnit.MILLISECONDS.sleep(100)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test1", 0), 0, 3)))

    println(manager.currentCache)
  }

  @Test
  def gatLagTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_KEY -> "zookeeper",
      POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )
    val conf = new OffsetsManagerConf(mconf)

    val manager = new OffsetsManager(conf)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test1", 0), 0, 1)))
    TimeUnit.MILLISECONDS.sleep(100)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test2", 0), 0, 3)))

    println(manager.currentCache)
    println(manager.getLag())
  }

  @Test
  def updateOffsetsTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_KEY -> "zookeeper",
      POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato_zoo",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )
    val conf = new OffsetsManagerConf(mconf)

    val manager = new OffsetsManager(conf)

    println(manager.committedOffsets(false))

    manager.updateOffsets(Map(
      TopicAndPartition("test1", 0) -> 1,
      TopicAndPartition("test2", 0) -> 5
    ))

    println(manager.committedOffsets(false))
  }

  @Test
  def updateOffsetsByDelayTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_KEY -> "zookeeper",
      POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato_zoo",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )
    val conf = new OffsetsManagerConf(mconf)

    val manager = new OffsetsManager(conf)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test1", 0), 0, 1)))
    TimeUnit.MILLISECONDS.sleep(5000)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test2", 0), 0, 3)))

    println("--------" + manager.currentCache)

    println("--------" + manager.committedOffsets())

    manager.updateOffsetsByDelay(System.currentTimeMillis(), 2500)
    println("--------" + manager.currentCache)

    println("--------" + manager.committedOffsets())
  }

  @Test
  def updateOffsetsOnKafkaTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_KEY -> "kafka",
      POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato_kafka",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )
    val conf = new OffsetsManagerConf(mconf)

    val manager = new OffsetsManager(conf)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test1", 0), 0, 1)))
    TimeUnit.MILLISECONDS.sleep(5000)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test2", 0), 0, 3)))

    println("--------" + manager.currentCache)

    println("--------" + manager.committedOffsets(false))

    manager.updateOffsetsByDelay(System.currentTimeMillis(), 2500)
    println("--------" + manager.currentCache)

    println("--------" + manager.committedOffsets(false))
  }
}
