package potato.kafka010.writer

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.Metadata
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.Test
import potato.kafka010.sink.ProducerRecord
import potato.spark.sql.writer._

class KafkaWriterTest {
  @Test
  def writeTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._

    val df = spark.createDataset(Seq(
      Record(1, "a", "hello"),
      Record(2, "b", "word"),
      Record(3, "c", "!")
    )).toDF()

    while (true) {
      df.potatoWrite.format("kafka")
        .option("topic", "test_out")
        //            .option("df.format", "first")
        .option("df.format", "json")
        //            .option("df.format", "csv")
        //            .option("df.sep", "\01")
        .option("failed.threshold", "1")
        .option(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "test02:9092")
        .save()
    }

    TimeUnit.DAYS.sleep(1)
  }

}

case class Record(id: Long, key: String, value: String)
