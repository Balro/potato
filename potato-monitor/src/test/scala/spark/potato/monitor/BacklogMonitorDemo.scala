package spark.potato.monitor

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BacklogMonitorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(10))
    val monitor = new BacklogMonitor(ssc)
    monitor.start()
  }
}
