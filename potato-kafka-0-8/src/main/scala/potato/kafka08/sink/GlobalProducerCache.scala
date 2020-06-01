package potato.kafka08.sink

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import spark.potato.common.pool.KeyedCacheBase

/**
 * 根据提供的Properties作为唯一key缓存KafkaProducer。
 */
object GlobalProducerCache extends KeyedCacheBase[Properties, KafkaProducer[Any, Any]] {
  def getCachedProducer[K, V](key: Properties): KafkaProducer[K, V] = this.synchronized {
    internalGetOrCreate(key) { () => new KafkaProducer[Any, Any](key) }.asInstanceOf[KafkaProducer[K, V]]
  }

  /**
   * 提供快速访问producer的方法。
   */
  def withProducer[K, V, R](prop: Properties)(f: KafkaProducer[K, V] => R): R = {
    val producer = getCachedProducer[K, V](prop)
    val ret = f(producer)
    producer.flush()
    ret
  }

  def close(): Unit = {
    internalClose { producer =>
      producer.close()
    }
  }
}
