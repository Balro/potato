package potato.kafka010.sink

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import potato.common.pool.KeyedCacheBase
import potato.kafka010.conf._

/**
 * 根据提供的Properties作为唯一key缓存KafkaProducer。
 */
object GlobalProducerCache extends KeyedCacheBase[Properties, KafkaProducer[Any, Any]] {
  def getCachedProducer[K, V](key: Properties): KafkaProducer[K, V] = this.synchronized {
    internalGetOrCreate(key) { () =>
      key.getProperty(POTATO_KAFKA_PRODUCER_SPEED_LIMIT_KEY.substring(POTATO_KAFKA_PRODUCER_PREFIX.length)) match {
        case _: String => new SpeedLimitedProducer[K, V](key).asInstanceOf[KafkaProducer[Any, Any]]
        case _ => new KafkaProducer[K, V](key).asInstanceOf[KafkaProducer[Any, Any]]
      }
    }.asInstanceOf[KafkaProducer[K, V]]
  }

  /**
   * 提供快速访问producer的方法。
   */
  def withProducer[K, V, R](props: Properties)(f: KafkaProducer[K, V] => R): R = {
    val producer = getCachedProducer[K, V](props)
    val ret = f(producer)
    producer.flush()
    ret
  }

  def close(): Unit = internalClose()

  override protected def clean(v: KafkaProducer[Any, Any]): Unit = v.close()
}
