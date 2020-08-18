package potato.kafka010.sql.writer

import java.util.Properties

import org.apache.spark.sql.DataFrame
import potato.kafka010.sink.{KafkaSinkUtil, ProducerRecord}
import potato.spark.sql.writer.PotatoDataSourceWriter

/**
 * 将df中的第一个字段值写入给定topic。
 */
class KafkaTopicWriter(df: DataFrame, topic: String, props: Properties) extends PotatoDataSourceWriter {
  override def write(): Unit = {
    val recordRDD = df.rdd.flatMap { f =>
      val first = f.get(0)
      if (first == null) {
        None
      } else {
        Some(new ProducerRecord[String, String](topic, first.toString))
      }
    }
    KafkaSinkUtil.saveToKafka(recordRDD, props)
  }
}
