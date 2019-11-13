package spark.streaming.potato.source.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import spark.streaming.potato.conf.ConfigKeys
import spark.streaming.potato.source.SourceUtil

object KafkaSourceUtil extends SourceUtil[(String, String)] {
  override def createDstream(ssc: StreamingContext): DStream[(String, String)] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      ssc.sparkContext.getConf.getAllWithPrefix(ConfigKeys.POTATO_SOURCE_KAFKA_CONSUMER_PARAMS_PREFIX).toMap,
      ssc.sparkContext.getConf.get(ConfigKeys.POTATO_SOURCE_KAFKA_TOPICS).split(",").map(_.trim).toSet)
  }
}

class KafkaSource {
  
}
