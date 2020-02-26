package spark.potato.kafka.sink

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object KafkaSinkUtil {
  def saveToKafka[K, V](stream: DStream[ProducerRecord[K, V]], props: Properties): Unit = {
    stream.foreachRDD { rdd =>
      saveToKafka(rdd, props)
    }
  }

  def saveToKafka[K, V](rdd: RDD[ProducerRecord[K, V]], props: Properties): Unit = {
    rdd.foreachPartition { part =>
      GlobalCachedProducer.withProducer[K, V, Unit](props) { producer =>
        part.foreach { item =>
          producer.send(item)
        }
        producer.flush()
      }
    }
  }
}
