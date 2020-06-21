package potato.kafka010.sink

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import potato.kafka010.conf._
import potato.kafka010.exception._

/**
 * 数据写入kafka的工具类。
 * producer相关参数(取自KafkaProducer_JavaDoc):
 * ["bootstrap.servers","acks","retries","batch.size","linger.ms","buffer.memory","key.serializer","value.serializer"]
 */
object KafkaSinkUtil {
  def saveToKafka[K, V](stream: DStream[ProducerRecord[K, V]], props: Properties): Unit = {
    stream.foreachRDD { rdd =>
      saveToKafka(rdd, props)
    }
  }

  def saveToKafka[K, V](rdd: RDD[ProducerRecord[K, V]], props: Properties): Unit = {
    rdd.foreachPartition { part =>
      var total = 0L
      var failed = 0L
      GlobalProducerCache.withProducer[K, V, Unit](props) { producer =>
        part.foreach { f =>
          producer.send(f, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception != null) {
                exception.printStackTrace()
                failed += 1
              }
            }
          })
          total += 1
        }
        producer.flush()
        val threshold = props.getProperty("failed.threshold", POTATO_KAFKA_PRODUCER_FAILED_THRESHOLD_DEFAULT).toDouble
        if (total > 0 && failed.toDouble / total > threshold) {
          throw SinkFailedException(f"Sink failed record in partition[${TaskContext.getPartitionId()}],failed/total[$failed/$total] reach threshold $threshold%.2f")
        }
      }
    }
  }
}
