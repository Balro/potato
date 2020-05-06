package spark.potato.quickstart.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import spark.potato.common.spark.StreamingContextUtil
import spark.potato.template.streaming.StreamingTemplate

import scala.collection.mutable

object StreamingDemo extends StreamingTemplate {
  private val queue = mutable.Queue.empty[RDD[String]]

  override def doWork(): Unit = {
    val ssc = createStreamingContext()
    val stream = ssc.queueStream(queue)
    stream.print()
    start(ssc)
  }

  override def afterStart(ssc: StreamingContext): Unit = {
    while (!ssc.sparkContext.isStopped) {
      queue += ssc.sparkContext.makeRDD(Seq("test data: " + new Date().toString))
      TimeUnit.MILLISECONDS.sleep(StreamingContextUtil.getBatchDuration(ssc).milliseconds)
    }
  }
}

