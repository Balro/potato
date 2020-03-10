package spark.potato.kafka.offsets

import kafka.common.InvalidConfigException
import org.apache.spark.SparkConf
import org.junit.Test
import spark.potato.kafka.offsets.manager.OffsetsManagerConf
import spark.potato.kafka.conf._

class OffsetsManagerConfTest {
  @Test
  def requiredKeyTest(): Unit = {
    try {
      new OffsetsManagerConf(new SparkConf(), Map.empty[String, String])
    } catch {
      case e: InvalidConfigException => println(e)
    }
  }

  @Test
  def mapKeyTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_KEY -> "zookeeper",
      POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY -> "test",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY -> "test01:9092"
    )
    val conf = new OffsetsManagerConf(mconf)
    println(conf.toMap)
    println(conf.get(POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY))
  }

  @Test
  def kvTest(): Unit = {
    val mconf = Map[String, String](
      POTATO_KAFKA_OFFSETS_STORAGE_KEY -> "zookeeper",
      POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY -> "test1,test2",
      POTATO_KAFKA_CONSUMER_GROUP_ID_KEY -> "test",
      POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> "earliest",
      POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY -> "test01:9092"
    )
    val conf = new OffsetsManagerConf(mconf)
    println(conf.storageType)
    println(conf.subscribeTopics)
    println(conf.groupId)
    println(conf.offsetResetPolicy)
    println(conf.bootstrapServers)
    println(conf.offsetsAutoUpdate)
    println(conf.offsetsAutoUpdateDelay)
  }
}
