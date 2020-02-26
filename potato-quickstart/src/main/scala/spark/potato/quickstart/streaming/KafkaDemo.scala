package spark.potato.quickstart.streaming

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.potato.kafka.source._

object KafkaDemo extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(10))

    val (stream, manager) = KafkaSourceUtil.valueDStream(ssc)

    stream.foreachRDD { (rdd, time) =>
      rdd.foreach(r => println(r))
      manager.updateOffsetsByDelay(time.milliseconds)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}