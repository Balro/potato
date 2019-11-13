package spark.streaming.potato.source

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait SourceUtil[T] {
  def createDstream(ssc: StreamingContext): DStream[T]
}
