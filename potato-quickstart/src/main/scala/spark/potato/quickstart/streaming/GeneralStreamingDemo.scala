package spark.potato.quickstart.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import spark.potato.template.streaming.StreamingTemplate

import scala.collection.mutable

object GeneralStreamingDemo extends StreamingTemplate {
  private val queue = mutable.Queue.empty[RDD[String]]

  override def doWork(): Unit = {
    val stream = ssc.queueStream(queue)
    stream.print()
  }

  override def afterStart(): Unit = {
    super.afterStart()
    while (!sc.isStopped) {
      queue += sc.makeRDD(Seq("test data: " + new Date().toString))
      TimeUnit.SECONDS.sleep(5)
    }
  }
}

