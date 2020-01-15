package spark.streaming.potato.plugins.kafka.offsets

import java.util.concurrent.TimeUnit

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.OffsetRange
import org.junit.Test

class OffsetsManagerTest {
  @Test
  def gatStartOffsetsTest(): Unit = {
    val mconf = Map[String, String](
      "spark.potato.source.kafka.offsets.storage" -> "zookeeper",
      "spark.potato.source.kafka.subscribe.topics" -> "test1,test2",
      "spark.potato.source.kafka.consumer.group.id" -> "test_potato",
      "spark.potato.source.kafka.consumer.auto.offset.reset" -> "earliest",
      "spark.potato.source.kafka.consumer.bootstrap.servers" -> "test03:9092"
    )
    val conf = new OffsetsManagerConf(mconf)

    val manager = new OffsetsManager(conf)

    println(manager.committedOffsets())
  }


  @Test
  def cacheOffsetsTest(): Unit = {
    val mconf = Map[String, String](
      "spark.potato.source.kafka.offsets.storage" -> "zookeeper",
      "spark.potato.source.kafka.subscribe.topics" -> "test1,test2",
      "spark.potato.source.kafka.consumer.group.id" -> "test_potato",
      "spark.potato.source.kafka.consumer.auto.offset.reset" -> "earliest",
      "spark.potato.source.kafka.consumer.bootstrap.servers" -> "test03:9092"
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
      "spark.potato.source.kafka.offsets.storage" -> "zookeeper",
      "spark.potato.source.kafka.subscribe.topics" -> "test1,test2",
      "spark.potato.source.kafka.consumer.group.id" -> "test_potato",
      "spark.potato.source.kafka.consumer.auto.offset.reset" -> "earliest",
      "spark.potato.source.kafka.consumer.bootstrap.servers" -> "test03:9092"
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
      "spark.potato.source.kafka.offsets.storage" -> "zookeeper",
      "spark.potato.source.kafka.subscribe.topics" -> "test1,test2",
      "spark.potato.source.kafka.consumer.group.id" -> "test_potato_zoo",
      "spark.potato.source.kafka.consumer.auto.offset.reset" -> "earliest",
      "spark.potato.source.kafka.consumer.bootstrap.servers" -> "test03:9092"
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
      "spark.potato.source.kafka.offsets.storage" -> "zookeeper",
      "spark.potato.source.kafka.subscribe.topics" -> "test1,test2",
      "spark.potato.source.kafka.consumer.group.id" -> "test_potato_zoo",
      "spark.potato.source.kafka.consumer.auto.offset.reset" -> "earliest",
      "spark.potato.source.kafka.consumer.bootstrap.servers" -> "test03:9092"
    )
    val conf = new OffsetsManagerConf(mconf)

    val manager = new OffsetsManager(conf)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test1", 0), 0, 1)))
    TimeUnit.MILLISECONDS.sleep(100)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test2", 0), 0, 3)))

    println(manager.currentCache)

    println(manager.committedOffsets())

    manager.updateOffsetsByDelay(50)
    println(manager.currentCache)

    println(manager.committedOffsets())
  }

  @Test
  def updateOffsetsOnKafkaTest(): Unit = {
    val mconf = Map[String, String](
      "spark.potato.source.kafka.offsets.storage" -> "kafka",
      "spark.potato.source.kafka.subscribe.topics" -> "test1,test2",
      "spark.potato.source.kafka.consumer.group.id" -> "test_potato_kafka",
      "spark.potato.source.kafka.consumer.auto.offset.reset" -> "earliest",
      "spark.potato.source.kafka.consumer.bootstrap.servers" -> "test03:9092"
    )
    val conf = new OffsetsManagerConf(mconf)

    val manager = new OffsetsManager(conf)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test1", 0), 0, 1)))
    TimeUnit.MILLISECONDS.sleep(100)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(TopicAndPartition("test2", 0), 0, 3)))

    println(manager.currentCache)

    println(manager.committedOffsets(false))

    manager.updateOffsetsByDelay(0)
    println(manager.currentCache)

    println(manager.committedOffsets(false))
  }
}
