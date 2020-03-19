package spark.potato.monitor.backlog

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._
import spark.potato.common.context.StreamingContextUtil
import spark.potato.common.exception.PotatoException
import spark.potato.common.service.StreamingService
import spark.potato.common.tools.DaemonThreadFactory
import spark.potato.monitor.reporter.{DingReporter, Reporter}
import spark.potato.monitor.conf._

/**
 * streaming积压监控服务。
 */
class BacklogMonitorService extends StreamingService with StreamingListener with Runnable with Logging {
  override val serviceName: String = POTATO_MONITOR_BACKLOG_MONITOR_SERVICE_NAME

  private var ssc: StreamingContext = _
  private var conf: BacklogMonitorConfig = _
  private var checkInterval: Long = _
  private var reporter: Reporter = _

  override def serve(ssc: StreamingContext): BacklogMonitorService = {
    this.ssc = ssc
    conf = BacklogMonitorConfig.parse(ssc.sparkContext.getConf)
    checkInterval = {
      if (conf.checkInterval < 0)
        StreamingContextUtil.getBatchDuration(ssc).milliseconds
      else conf.checkInterval
    }
    reporter = conf.reporter match {
      case "ding" => new DingReporter(ssc.sparkContext.getConf.getAll.toMap)
      case unknown => throw new PotatoException(s"Unknown reporter $unknown")
    }

    ssc.addStreamingListener(this)
    this
  }

  private val delayBatch = new AtomicLong(0)
  private var lastBatchTime: Long = System.currentTimeMillis()
  private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)

  /**
   * 初始化上次批次执行时间戳。
   */
  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    lastBatchTime = streamingStarted.time
  }

  /**
   * 计算积压批次数量。
   */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    delayBatch.incrementAndGet()
  }

  /**
   * 计算积压批次数量，更新上次批次执行时间戳。
   */
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    lastBatchTime = batchCompleted.batchInfo.submissionTime
    delayBatch.decrementAndGet()
  }

  private val started = new AtomicBoolean(false)

  override def start(): Unit = {
    if (started.get()) {
      logWarning("Service BacklogMonitor already started.")
      return
    }
    logInfo("Start BacklogMonitor.")
    started.set(true)
    executor.scheduleAtFixedRate(this, checkInterval, checkInterval, TimeUnit.MILLISECONDS)
    logInfo("BacklogMonitor started.")
  }

  override def stop(): Unit = {
    logInfo("Stop BacklogMonitor.")
    executor.shutdownNow()
    logInfo("BacklogMonitor stopped.")
  }

  // 是否已经进行过了汇报，用于计算汇报间隔。
  private var reported = false
  // 已连续汇报次数，用于计算汇报间隔。
  private var reportedTimes = 0
  // 上次汇报时间，用于计算汇报间隔。
  private var lastReportedTime = -1L

  override def run(): Unit = {
    val current = System.currentTimeMillis()
    val currentDelay = current - lastBatchTime
    if (
    // 积压时间已达到阈值以下，汇报正常状态。
      reported && currentDelay < conf.threshold
    ) {
      logInfo(s"Total delay is lower than threshold, current delay $currentDelay.")
      reported = false
      reportedTimes = 0
      reporter.report(copywriting(currentDelay))
    } else if (
    // 未达到最大汇报次数。
      reportedTimes < conf.reportedMax
        // 等待时间满足汇报间隔。
        && current - lastReportedTime > conf.reportedInterval
        // 延迟时间仍在阈值以上。
        && currentDelay > conf.threshold) {
      logInfo(s"Total delay is upper than threshold, current delay $currentDelay, reportedTimes $reportedTimes.")
      reported = true
      reportedTimes += 1
      lastReportedTime = current
      reporter.report(copywriting(currentDelay))
    }
  }

  private val df = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")

  /**
   * 汇报文案。
   */
  private def copywriting(currentDelay: Long): String =
    s"""${if (reported) "作业出现积压!" else "作业恢复正常"}
       |
       |reportedNum   : $reportedTimes#
       |reportTime    : ${df.format(new Date())}
       |appName       : ${ssc.sparkContext.appName}
       |appId         : ${ssc.sparkContext.applicationId}
       |webui         : ${ssc.sparkContext.uiWebUrl.getOrElse("unknown")}
       |delayBatch    : ${delayBatch.get()}
       |delayTime     : ${currentDelay / 1000}s
       |threshold     : ${conf.threshold / 1000}s
       |""".stripMargin
}

/**
 * BacklogMonitor参数包装。
 *
 * @param reporter         reporter类型。
 * @param threshold        积压时间阈值。
 * @param checkInterval    检查间隔。
 * @param reportedInterval 汇报间隔。
 * @param reportedMax      最大汇报次数。
 */
case class BacklogMonitorConfig(reporter: String,
                                threshold: Long,
                                checkInterval: Long,
                                reportedInterval: Long,
                                reportedMax: Int)

object BacklogMonitorConfig {
  def parse(conf: SparkConf): BacklogMonitorConfig = {
    import spark.potato.common.conf._
    import spark.potato.monitor.conf._
    BacklogMonitorConfig(
      conf.get(POTATO_MONITOR_BACKLOG_REPORTER_TYPE_KEY, POTATO_MONITOR_BACKLOG_REPORTER_TYPE_DEFAULT),
      conf.get(POTATO_MONITOR_BACKLOG_DELAY_MS_KEY).toLong,
      conf.getLong(POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY, -1L),
      conf.getLong(POTATO_MONITOR_BACKLOG_REPORTER_INTERVAL_MS_KEY, POTATO_MONITOR_BACKLOG_REPORTER_INTERVAL_MS_DEFAULT),
      conf.getInt(POTATO_MONITOR_BACKLOG_REPORTER_MAX_KEY, POTATO_MONITOR_BACKLOG_REPORTER_MAX_DEFAULT)
    )
  }
}