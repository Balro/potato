package potato.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, Row}

package object writer {

  class PotatoWriterFunction[T](ds: Dataset[T]) {
    def potatoWrite: PotatoDataFrameWriter[T] = {
      new PotatoDataFrameWriter(ds)
    }
  }

  implicit def toPotatoWriter[T](ds: Dataset[T]): PotatoWriterFunction[T] = new PotatoWriterFunction(ds)
}
