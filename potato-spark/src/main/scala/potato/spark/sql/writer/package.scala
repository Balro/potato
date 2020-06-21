package potato.spark.sql

import org.apache.spark.sql.DataFrame

package object writer {
  // 使用反射，为了避免potato-spark项目依赖其他插件而产生循环依赖。
  val writerMapping = Map(
    "kafka" -> "potato.kafka010.writer.KafkaWriter"
  )

  class PotatoWriterFunction(df:DataFrame) {
    def potatoWrite:PotatoWriterManager = {
      new PotatoWriterManager(df)
    }
  }

  implicit def toPotatoWriterManager(ds: DataFrame): PotatoWriterFunction = new PotatoWriterFunction(ds)
}
