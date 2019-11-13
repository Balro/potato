import java.util.Collections

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.asScalaIterator

object Test {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "bootstrap.servers" -> "test01:9092",
      "group.id" -> "baluo_test_kf",
      "enable.auto.commit" -> "true",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val consumer = new KafkaConsumer[String, String](config)

    consumer.subscribe(Collections.singletonList("test_topic"))

    while (true) {
      val rs = consumer.poll(100)
      for (r <- rs.iterator())
        println(r.value())
    }
  }
}
