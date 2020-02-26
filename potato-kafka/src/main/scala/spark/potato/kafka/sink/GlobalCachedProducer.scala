package spark.potato.kafka.sink

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.apache.kafka.clients.producer.KafkaProducer

/**
 * KafkaProducer 线程安全，官方doc描述，多线程共享一个实例比单独持有实例效率更高。
 * 故改缓存设置为单jvm(executor)内多task共享一个producer。
 * 若想提高并发，请增加executor数量。
 */
object GlobalCachedProducer {
  private val producers = new ConcurrentHashMap[Properties, KafkaProducer[Any,Any]]()

  private def getProducer[K, V](prop: Properties): KafkaProducer[K, V] = synchronized {
    import scala.collection.JavaConversions.mapAsScalaConcurrentMap
    producers.getOrElseUpdate(prop, new KafkaProducer[K, V](prop).asInstanceOf[KafkaProducer[Any,Any]])
      .asInstanceOf[KafkaProducer[K, V]]
  }

  def withProducer[K, V, R](prop: Properties)(f: KafkaProducer[K, V] => R): R = {
    val producer = getProducer[K, V](prop)
    val ret = f(producer)
    producer.flush()
    ret
  }
}