package potato.spark.monitor.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

import scala.collection.mutable
import potato.common.conf._
import potato.spark.conf._

class StreamingBacklogMonitorServiceTest {
  @Test
  def test(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
      .set(POTATO_MONITOR_STREAMING_BACKLOG_THRESHOLD_MS_KEY, 5000.toString)
      .set(POTATO_MONITOR_STREAMING_BACKLOG_REPORT_INTERVAL_MS_KEY, 10000.toString)
      .set(POTATO_COMMON_SENDER_DING_TOKEN_KEY, "2aa09020785483")
    val ssc = new StreamingContext(conf, Seconds(5))
    val service = new StreamingBacklogMonitorService()
    service.serve(ssc)

    val queue = new mutable.Queue[RDD[Long]]()
    ssc.queueStream(queue).map { f =>
      if (TaskContext.get().stageId() < 50 || TaskContext.get().stageId() > 100)
        TimeUnit.SECONDS.sleep(8)
      f
    }.print()
    ssc.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        queue.+=(ssc.sparkContext.parallelize(Seq(System.currentTimeMillis())))
      }
    })

    service.startAndStopWhenShutdown()

    ssc.start()
    queue.+=(ssc.sparkContext.parallelize(Seq(System.currentTimeMillis())))

    ssc.awaitTermination()
  }
}
