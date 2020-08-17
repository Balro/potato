package potato.kafka010.writer

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import potato.kafka010.sink.{KafkaSinkUtil, ProducerRecord}
import potato.spark.sql.writer.PotatoWriter
import potato.kafka010.writer.KafkaWriteFormat._

class KafkaWriter extends PotatoWriter with Serializable with Logging {
  private var topic: String = _
  private var format: KafkaWriteFormat = _
  private var sep: String = _
  private val props = new Properties()

  override def option(options: Map[String, String]): PotatoWriter = {
    import scala.collection.JavaConversions.propertiesAsScalaMap
    topic = options("topic")
    format = KafkaWriteFormat.withName(options("writer.format"))
    sep = options.getOrElse("csv.sep", ",")
    this.props ++= Map(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName
    ) ++= options
    this
  }

  override def mode(mode: SaveMode): PotatoWriter = {
    mode match {
      case SaveMode.Append =>
      case other => logWarning(s"KafkaWriter only support append mode, $other will transform to append implicitly.")
    }
    this
  }

  override def save(ds: DataFrame): Unit = {
    assert(topic != null)
    val schema = ds.schema
    val transformedRDD = format match {
      case FIRST => ds.rdd
      case CSV => ds.selectExpr(s"concat_ws('$sep',${schema.map(_.name).mkString(",")})").rdd
      case JSON => ds.selectExpr(s"to_json(struct(${schema.map(_.name).mkString(",")}))").rdd
      case other => throw new KafkaException(s"Unsupported kafka writer df format $other.")
    }
    val recordRDD = transformedRDD.flatMap { f =>
      val first = f.get(0)
      if (first == null)
        None
      else
        Some(first.toString)
    }.map(f => new ProducerRecord[String, String](topic, f))
    KafkaSinkUtil.saveToKafka(recordRDD, props)
  }

}
