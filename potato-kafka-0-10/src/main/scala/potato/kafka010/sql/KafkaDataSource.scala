package potato.kafka010.sql

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.{DataFrame, SaveMode}
import potato.common.exception.PotatoException
import potato.kafka010.sql.writer.KafkaTopicWriter
import potato.spark.sql.PotatoDataSource
import potato.spark.sql.writer.PotatoDataSourceWriter

class KafkaDataSource extends PotatoDataSource {
  override def createWriter(df: DataFrame, mode: SaveMode, options: Map[String, String]): PotatoDataSourceWriter = {
    if (mode != SaveMode.Append) {
      throw new PotatoException("KafkaDataSourceWriter only support append mode")
    }

    val props = new Properties()
    scala.collection.JavaConversions.propertiesAsScalaMap(props) ++= Map(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName
    ) ++= options

    new KafkaTopicWriter(df, options.getOrElse("topic", throw new PotatoException("prop 'topic' required")), props)
  }
}
