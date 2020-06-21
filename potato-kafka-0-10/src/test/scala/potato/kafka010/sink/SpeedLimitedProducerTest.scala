package potato.kafka010.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.Test
import potato.kafka010.conf.POTATO_KAFKA_PRODUCER_SPEED_LIMIT_KEY

class SpeedLimitedProducerTest {
  @Test
  def test0(): Unit = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "test02:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(POTATO_KAFKA_PRODUCER_SPEED_LIMIT_KEY, "1000")

    val producer = new SpeedLimitedProducer[String, String](props)

    for (i <- 0 until 10000) {
      producer.send(new ProducerRecord[String, String]("test_out", i.toString))
      println(i)
    }
  }
}
