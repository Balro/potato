package potato.kafka010.sql

import org.apache.spark.sql.{DataFrame, SaveMode}
import potato.common.exception.PotatoException
import potato.kafka010.conf.PotatoKafkaConf
import potato.kafka010.sql.writer.KafkaTopicWriter
import potato.spark.sql.PotatoDataSource
import potato.spark.sql.writer.PotatoDataSourceWriter

class KafkaDataSource extends PotatoDataSource {
  override def createWriter(df: DataFrame, mode: SaveMode, options: Map[String, String]): PotatoDataSourceWriter = {
    if (mode != SaveMode.Append) {
      throw new PotatoException("KafkaDataSourceWriter only support append mode")
    }

    val props = new PotatoKafkaConf(df.sparkSession.sparkContext.getConf, options).toProducerProperties

    new KafkaTopicWriter(df, options.getOrElse("topic", throw new PotatoException("prop 'topic' required")), props)
  }
}
