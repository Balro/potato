package spark.streaming.potato.plugins.monitor

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchSubmitted, StreamingListenerStreamingStarted}
import spark.streaming.potato.common.exception.PotatoException
import spark.streaming.potato.common.traits.Service
import spark.streaming.potato.common.utils.DaemonThreadFactory
import spark.streaming.potato.plugins.monitor.reporter.{DingReporter, Reporter}

class BacklogMonitor(ssc: StreamingContext) extends StreamingListener with Runnable with Service with Logging {
  ssc.addStreamingListener(this)
  private val delayBatch = new AtomicLong(0)
  private var lastBatchTime: Long = -1
  private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)
  private val conf = BacklogMonitorConfig.parse(ssc.sparkContext.getConf)
  private val reporter: Reporter = conf.reporter match {
    case "ding" => new DingReporter(ssc.sparkContext.getConf.getAll.toMap)
    case unknown => throw new PotatoException(s"Unknown reporter $unknown")
  }

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    lastBatchTime = streamingStarted.time
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    delayBatch.incrementAndGet()
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    lastBatchTime = batchCompleted.batchInfo.submissionTime
    delayBatch.decrementAndGet()
  }

  def start(): Unit = {
    logInfo("Start BacklogMonitor.")
    executor.scheduleAtFixedRate(this, conf.checkInterval, conf.checkInterval, TimeUnit.MILLISECONDS)
    logInfo("BacklogMonitor started.")
  }

  override def stop(): Unit = {
    logInfo("Stop BacklogMonitor.")
    executor.shutdownNow()
    logInfo("BacklogMonitor stopped.")
  }

  private var reported = false
  private var reportedTimes = 0
  private var lastReportedTime = -1L

  override def run(): Unit = {
    if (lastBatchTime < 0) {
      logWarning("LastSuccessTime is not initialized.")
      return
    }
    val current = System.currentTimeMillis()
    val currentDelay = current - lastBatchTime
    if (reported && currentDelay < conf.threshold) {
      logInfo(s"Total delay is lower than threshold, current delay $currentDelay.")
      reported = false
      reportedTimes = 0
      reporter.report(copywriting(currentDelay))
    } else if (reportedTimes < conf.reportedMax
      && current - lastReportedTime > conf.reportedInterval
      && currentDelay > conf.threshold) {
      logInfo(s"Total delay is upper than threshold, current delay $currentDelay, reportedTimes $reportedTimes.")
      reported = true
      reportedTimes += 1
      lastReportedTime = current
      reporter.report(copywriting(currentDelay))
    }
  }

  private val df = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")

  private def copywriting(currentDelay: Long): String =
    s"""${if (reported) "作业出现积压!" else "作业恢复正常"}
       |
       |reportedNum   : $reportedTimes#
       |reportTime    : ${df.format(new Date())}
       |appName       : ${ssc.sparkContext.appName}
       |appId         : ${ssc.sparkContext.applicationId}
       |delayBatch    : ${delayBatch.get()}
       |delayTime     : ${currentDelay / 1000}s
       |threshold     : ${conf.threshold / 1000}s
       |""".stripMargin
}

case class BacklogMonitorConfig(reporter: String,
                                threshold: Long,
                                checkInterval: Long,
                                reportedInterval: Long,
                                reportedMax: Int)

object BacklogMonitorConfig {
  def parse(conf: SparkConf): BacklogMonitorConfig = {
    import MonitorConfigKeys._
    import spark.streaming.potato.common.conf.CommonConfigKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY
    BacklogMonitorConfig(
      conf.get(BACKLOG_REPORTER_TYPE_KEY, BACKLOG_REPORTER_TYPE_DEFAULT),
      conf.get(MONITOR_BACKLOG_DELAY_SECONDS_KEY).toLong * 1000,
      conf.get(POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY).toLong * 1000,
      conf.getLong(BACKLOG_REPORTER_INTERVAL_SECOND_KEY, BACKLOG_REPORTER_INTERVAL_SECOND_DEFAULT) * 1000,
      conf.getInt(BACKLOG_REPORTER_MAX_KEY, BACKLOG_REPORTER_MAX_DEFAULT)
    )
  }
}