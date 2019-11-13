package spark.streaming.potato.source.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import spark.streaming.potato.source.SourceUtil

//object KafkaSourceUtil extends SourceUtil[(String, String)] {
//  override def createDstream(ssc: StreamingContext): DStream[(String, String)] = {
//    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map(), Set())
//  }
//}
