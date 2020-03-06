package spark.potato.monitor.backlog

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BacklogMonitorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(10))
    val monitor = new BacklogMonitor().serve(ssc)
    monitor.start()
  }
}
