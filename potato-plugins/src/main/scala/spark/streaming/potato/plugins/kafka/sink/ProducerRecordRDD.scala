package spark.streaming.potato.plugins.kafka.sink

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD
import GlobalCachedProducer._

class ProducerRecordRDD(rdd: RDD[ProducerRecord[String, String]]) extends Serializable {
  def saveToKafka(props: Properties): Unit = {
    rdd.foreachPartition { part =>
      withProducer(props) { producer =>
        part.foreach { record =>
          producer.send(record)
        }
      }
    }
  }
}
