package spark.streaming.potato.plugins.kafka.sink

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.apache.kafka.clients.producer.KafkaProducer

/**
 * KafkaProducer 线程安全，官方doc描述，多线程共享一个实例比单独持有实例效率更高。
 * 故改缓存设置为单jvm(executor)内多task共享一个producer。
 * 若想提高并发，请增加executor数量。
 */
object GlobalCachedProducer {
  private val producers = new ConcurrentHashMap[Properties, KafkaProducer[String, String]]()

  private def getProducer(prop: Properties): KafkaProducer[String, String] = synchronized {
    import scala.collection.JavaConversions.mapAsScalaConcurrentMap
    producers.getOrElseUpdate(prop, new KafkaProducer[String, String](prop))
  }

  def withProducer[R](prop: Properties)(f: KafkaProducer[String, String] => R): R = {
    val producer = getProducer(prop)
    val ret = f(producer)
    producer.flush()
    ret
  }
}
