package spark.potato.kafka

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import spark.potato.kafka.conf._
import spark.potato.common.conf.PropertiesUtil.{mapToProperties => mtp, confToProperties => ctp}

/**
 * 提供数据写kafka的隐式转换。
 */
package object sink {

  type ProducerRecord[K, V] = org.apache.kafka.clients.producer.ProducerRecord[K, V]

  class ProducerRecordRDDFunction[K, V](rdd: RDD[ProducerRecord[K, V]]) extends Serializable {
    def saveToKafka(props: Properties): Unit = {
      KafkaSinkUtil.saveToKafka(rdd, props)
    }
  }

  class ProducerRecordDStreamFunction[K, V](stream: DStream[ProducerRecord[K, V]]) extends Serializable {
    def saveToKafka(props: Properties): Unit = {
      KafkaSinkUtil.saveToKafka(stream, props)
    }
  }

  implicit def toProducerRecordRDDFunction[K, V](rdd: RDD[ProducerRecord[K, V]]): ProducerRecordRDDFunction[K, V] = {
    new ProducerRecordRDDFunction[K, V](rdd)
  }

  implicit def toProducerRecordDStreamFunction[K, V](stream: DStream[ProducerRecord[K, V]]): ProducerRecordDStreamFunction[K, V] = {
    new ProducerRecordDStreamFunction[K, V](stream)
  }

  implicit def mapToProperties(map: Map[String, String]): Properties = mtp(map)

  implicit def confToProperties(conf: SparkConf): Properties = ctp(conf, POTATO_KAFKA_PRODUCER_CONF_PREFIX)
}
