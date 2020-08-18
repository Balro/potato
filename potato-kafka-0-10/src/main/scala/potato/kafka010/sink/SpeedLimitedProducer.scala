package potato.kafka010.sink

import java.util.Properties
import java.util.concurrent.{Future, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, RecordMetadata}
import potato.kafka010.conf._

class SpeedLimitedProducer[K, V](props: Properties) extends KafkaProducer[K, V](props) {
  // 每秒中接受的最大send条数。
  private val limit: Long = props.getProperty(POTATO_KAFKA_PRODUCER_SPEED_LIMIT_KEY.substring(POTATO_KAFKA_PRODUCER_PREFIX.length), Long.MaxValue.toString).toLong
  private val curSize = new AtomicLong(0L)
  private var lastSleepTime = System.currentTimeMillis()

  override def send(record: ProducerRecord[K, V], callback: Callback): Future[RecordMetadata] = {
    trySleep()
    super.send(record, callback)
  }

  private def trySleep(): Unit = this.synchronized {
    /**
     * 逻辑：
     * * 如果当前批次大小超过阈值，则检查距上次休眠的时间。如果距上次休眠时间超过1秒，则说明此时距上次休眠时间内的速率未超过阈值。
     * * 否则则休眠至上次休眠时间的下一秒之后再进行send操作。
     */
    if (curSize.incrementAndGet() >= limit) {
      val current = System.currentTimeMillis()
      if (current - lastSleepTime < 1000) {
        val sleepTime = lastSleepTime + 1000
        TimeUnit.MILLISECONDS.sleep(sleepTime - current)
        lastSleepTime = sleepTime
      } else {
        lastSleepTime = current
      }
      curSize.set(0)
    }
  }
}