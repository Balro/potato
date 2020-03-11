package spark.potato.template.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import spark.potato.common.conf._
import spark.potato.lock.running.RunningLockManager
import spark.potato.monitor.backlog.BacklogMonitorService
import spark.potato.template.conf._
import spark.potato.lock.conf._
import spark.potato.monitor.conf._

object StreamingTemplateTest extends StreamingTemplate {
  private val queue = mutable.Queue.empty[RDD[String]]

  override def doWork(): Unit = {
    val stream = ssc.queueStream(queue)
    stream.foreachRDD { rdd =>
      rdd.foreach(println)
    }
  }

  override def afterStart(): Unit = {
    0 until 10 foreach { _ =>
      val rdd = sc.makeRDD(Seq(new Date().toString))
      queue += rdd
      println(s"Offer rdd $rdd, queue size ${queue.size}")
      TimeUnit.MILLISECONDS.sleep(5000)
    }
  }

  override def createConf(): SparkConf = {
    new SparkConf().setMaster("local[2]").setAppName("StreamingTemplateTest")
      .set(POTATO_STREAMING_BATCH_DURATION_MS_KEY, 5000.toString)
      .set(POTATO_TEMPLATE_ADDITIONAL_SERVICES_KEY,
        Seq(classOf[BacklogMonitorService], classOf[RunningLockManager]).map {
          _.getName
        }.mkString(","))
      .set(POTATO_MONITOR_BACKLOG_DELAY_MS_KEY, 1.toString)
      .set(POTATO_MONITOR_BACKLOG_REPORTER_DING_TOKEN_KEY, "abc")
      .set(POTATO_RUNNING_LOCK_ZOOKEEPER_QUORUM_KEY, "test02")
  }
}
