package potato.demo.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import potato.spark.template._

import scala.collection.mutable

object StreamingDemo extends FullTemplate {
  private val queue = mutable.Queue.empty[RDD[String]]

  override def main(args: Array[String]): Unit = {
    val ssc = createSSC().withDefaultService.stopWhenShutdown
    val stream = ssc.queueStream(queue)
    stream.print()

    ssc.start()
    val t = new Thread(new Runnable {
      override def run(): Unit = {
        while (!ssc.sparkContext.isStopped) {
          println(ssc.sparkContext.isStopped)
          queue += ssc.sparkContext.makeRDD(Seq("test data: " + new Date().toString))
          TimeUnit.MILLISECONDS.sleep(stream.slideDuration.milliseconds)
        }
      }
    })
    t.setDaemon(true)
    t.start()

    ssc.awaitTerminationOrTimeout(60000)
  }
}
