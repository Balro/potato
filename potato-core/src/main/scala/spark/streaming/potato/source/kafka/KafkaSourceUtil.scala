package spark.streaming.potato.source.kafka

import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import spark.streaming.potato.source.kafka.imp.KafkaBrokerOffsetManager

import scala.reflect.ClassTag

object KafkaSourceUtil extends Logging {
  def createDStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag, R: ClassTag]
  (offsetManagerType: String)
  (ssc: StreamingContext, kafkaParams: Map[String, String], messageHandler: MessageAndMetadata[K, V] => R): (InputDStream[R], KafkaOffsetManager) = {
    val cleanedParams = {
      val reset = {
        kafkaParams.getOrElse("auto.offset.reset", None) match {
          case "earliest" => "smallest"
          case "latest" => "largest"
          case None =>
            logWarning("kafka param \"auto.offset.reset\" not set, use largest instead.")
            "largest"
        }
      }
      kafkaParams + ("enable.auto.commit" -> "false", "auto.offset.reset" -> reset)
    }
    val offsetManager = offsetManagerType.toUpperCase match {
      case "KAFKA" => new KafkaBrokerOffsetManager(ssc, cleanedParams)
    }
    val stream = KafkaUtils.createDirectStream[K, V, KD, VD, R](
      ssc,
      cleanedParams,
      offsetManager.fromOffset,
      messageHandler)
    stream.transform(rdd => {
      offsetManager.cacheOffset(rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
      rdd
    })

    (stream, offsetManager)
  }

  def defaultDStream(ssc: StreamingContext, kafkaParams: Map[String, String]): (InputDStream[(String, String)], KafkaOffsetManager) = {
    createDStream[String, String, StringDecoder, StringDecoder, (String, String)](
      "kafka")(ssc, kafkaParams, m => (m.key(), m.message()))
  }
}
