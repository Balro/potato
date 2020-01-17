package spark.streaming.potato.plugins.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD
import spark.streaming.potato.plugins.kafka.sink.ProducerRecordRDD

object KafkaImplicits {
  implicit def toProducerRecordRDD(rdd: RDD[ProducerRecord[String, String]]): ProducerRecordRDD = {
    new ProducerRecordRDD(rdd)
  }
}
