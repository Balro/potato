package spark.potato.kafka.offsets

import kafka.common.InvalidConfigException
import org.junit.Test
import spark.potato.kafka.offsets.manager.OffsetsManagerConf

class OffsetsManagerConfTest {
  @Test
  def requiredKeyTest(): Unit = {
    val mconf = Map[String, String]()
    try {
      val conf = new OffsetsManagerConf(mconf)
    } catch {
      case e: InvalidConfigException => println(e)
    }
  }

  @Test
  def mapKeyTest(): Unit = {
    val mconf = Map[String, String](
      "spark.potato.source.kafka.offsets.storage" -> "zookeeper",
      "spark.potato.source.kafka.subscribe.topics" -> "test",
      "spark.potato.source.kafka.consumer.group.id" -> "test",
      "spark.potato.source.kafka.consumer.auto.offset.reset" -> "earliest",
      "spark.potato.source.kafka.consumer.bootstrap.servers" -> "test01:9092"
    )
    val conf = new OffsetsManagerConf(mconf)
    println(conf.get("auto.offset.reset"))
  }

  @Test
  def kvTest(): Unit = {
    val mconf = Map[String, String](
      "spark.potato.source.kafka.offsets.storage" -> "zookeeper",
      "spark.potato.source.kafka.subscribe.topics" -> "test1,test2",
      "spark.potato.source.kafka.consumer.group.id" -> "test",
      "spark.potato.source.kafka.consumer.auto.offset.reset" -> "earliest",
      "spark.potato.source.kafka.consumer.bootstrap.servers" -> "test01:9092"
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
