package potato.kafka010.offsets.manager

import java.util.concurrent.TimeUnit

import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.OffsetRange
import org.junit.Test
import spark.potato.kafka.conf._

class OffsetsManagerTest {
  val commonConf = Map(
    POTATO_KAFKA_COMMON_BOOTSTRAP_SERVERS_KEY -> "test03:9092",
    POTATO_KAFKA_CONSUMER_KEY_DESERIALIZER_KEY -> POTATO_KAFKA_CONSUMER_KEY_DESERIALIZER_DEFAULT,
    POTATO_KAFKA_CONSUMER_VALUE_DESERIALIZER_KEY -> POTATO_KAFKA_CONSUMER_VALUE_DESERIALIZER_DEFAULT
  )

  @Test
  def gatOffsetsTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY -> "zookeeper",
      //      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY -> "kafka",
      POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest"
    )
    val conf = new SparkConf()
    conf.setAll(commonConf ++ mconf)

    val manager = new OffsetsManager(conf)

    println(manager.committedOffsets())
    println(manager.committedOffsets(false))
  }


  @Test
  def cacheOffsetsTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY -> "zookeeper",
      POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_COMMON_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )

    val conf = new SparkConf()
    conf.setAll(commonConf ++ mconf)

    val manager = new OffsetsManager(conf)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(new TopicPartition("test1", 0), 0, 1)))
    TimeUnit.MILLISECONDS.sleep(100)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(new TopicPartition("test1", 0), 0, 3)))

    println(manager.currentCache)
  }

  @Test
  def gatLagTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY -> "kafka",
      //      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY -> "zookeeper",
      POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_COMMON_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )

    val conf = new SparkConf()
    conf.setAll(commonConf ++ mconf)

    val manager = new OffsetsManager(conf)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(new TopicPartition("test1", 0), 0, 1)))
    TimeUnit.MILLISECONDS.sleep(100)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(new TopicPartition("test2", 0), 0, 3)))

    println("current cache -> " + manager.currentCache)
    println("get lag -> " + manager.getLag())
    println("get lag by topic -> " + manager.getLagByTopic(Set("test1", "test2")))
  }

  @Test
  def updateOffsetsTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY -> "kafka",
      //      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY -> "zookeeper",
      POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato_zoo",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_COMMON_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )

    val conf = new SparkConf()
    conf.setAll(commonConf ++ mconf)

    val manager = new OffsetsManager(conf)

    println(manager.committedOffsets(false))

    manager.updateOffsets(Map(
      new TopicPartition("test1", 0) -> 1,
      new TopicPartition("test2", 0) -> 5
    ))

    println(manager.committedOffsets(false))
  }

  @Test
  def updateOffsetsByDelayTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY -> "kafka",
      //      POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY -> "zookeeper",
      POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test_potato_zoo",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_COMMON_BOOTSTRAP_SERVERS_KEY -> "test03:9092"
    )

    val conf = new SparkConf()
    conf.setAll(commonConf ++ mconf)

    val manager = new OffsetsManager(conf)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(new TopicPartition("test1", 0), 0, 1)))
    TimeUnit.MILLISECONDS.sleep(5000)
    manager.cacheOffsets(System.currentTimeMillis(),
      Seq(OffsetRange(new TopicPartition("test2", 0), 0, 3)))

    println("--------" + manager.currentCache)

    println("--------" + manager.committedOffsets())

    manager.updateOffsetsByDelay(System.currentTimeMillis(), 2500)
    println("--------" + manager.currentCache)

    println("--------" + manager.committedOffsets())
  }
}
