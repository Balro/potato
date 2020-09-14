package potato.kafka010.sink

import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import potato.kafka010.conf._
import potato.kafka010.exception._

/**
 * 数据写入kafka的工具类。
 * producer相关参数(取自KafkaProducer_JavaDoc):
 * ["bootstrap.servers","acks","retries","batch.size","linger.ms","buffer.memory","key.serializer","value.serializer"]
 */
object KafkaSinkUtil extends Logging {
  def saveToKafka[K, V](stream: DStream[ProducerRecord[K, V]], props: Properties): Unit = {
    stream.foreachRDD { rdd =>
      saveToKafka(rdd, props)
    }
  }

  def saveToKafka[K, V](rdd: RDD[ProducerRecord[K, V]], props: Properties): Unit = {
    rdd.foreachPartition { part =>
      val total = new AtomicLong(0)
      val failed = new AtomicLong(0)
      val record = props.getProperty(POTATO_KAFKA_PRODUCER_FAILED_RECORD_KEY.substring(POTATO_KAFKA_PRODUCER_PREFIX.length), POTATO_KAFKA_PRODUCER_FAILED_RECORD_DEFAULT).toLong
      val ratio = props.getProperty(POTATO_KAFKA_PRODUCER_FAILED_RATIO_KEY.substring(POTATO_KAFKA_PRODUCER_PREFIX.length), POTATO_KAFKA_PRODUCER_FAILED_RATIO_DEFAULT).toDouble
      val ratioBackoff = props.getProperty(POTATO_KAFKA_PRODUCER_FAILED_RATIO_BACKOFF_KEY.substring(POTATO_KAFKA_PRODUCER_PREFIX.length), POTATO_KAFKA_PRODUCER_FAILED_RATIO_BACKOFF_DEFAULT).toLong
      GlobalProducerCache.withProducer[K, V, Unit](props) { producer =>
        part.foreach { f =>
          producer.send(f, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              total.incrementAndGet()
              if (exception != null) {
                exception.printStackTrace()
                failed.incrementAndGet()
                if (failed.get() > record) {
                  throw SinkFailedException(f"Sink failed in partition[${TaskContext.getPartitionId()}] because failed record $failed over $record")
                } else if ((total.get() > ratioBackoff || total.get() > record) && failed.get().toDouble / total.get() > ratio) {
                  throw SinkFailedException(f"Sink failed in partition[${TaskContext.getPartitionId()}] because failed ratio $failed/$total=${failed.get().toDouble / total.get()}%.3f over $ratio%.3f")
                } else {
                  logWarning(s"Sink failed record status $failed/$total")
                }
              }
            }
          })
        }
        producer.flush()
        if (failed.get().toDouble / total.get() > ratio) {
          throw SinkFailedException(f"Sink failed in partition[${TaskContext.getPartitionId()}] because failed ratio $failed/$total=${failed.get().toDouble / total.get().toDouble}%.3f over $ratio%.3f")
        }
      }
    }
  }
}
