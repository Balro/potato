package spark.potato.monitor.streaming

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._
import spark.potato.common.exception.PotatoException
import spark.potato.common.spark.service.StreamingService
import spark.potato.common.spark.streaming.StreamingUtil
import spark.potato.common.threads.DaemonThreadFactory
import spark.potato.monitor.reporter.{MaxNumReporter, Reporter}
import spark.potato.monitor.conf._
import spark.potato.monitor.notify.DingRobotNotify

/**
 * streaming积压监控服务。
 */
class BacklogMonitorService extends StreamingService with StreamingListener with Runnable with Logging {
  override val serviceName: String = POTATO_MONITOR_BACKLOG_SERVICE_NAME

  private var ssc: StreamingContext = _
  private var conf: SparkConf = _
  private var threshold: Long = _
  private var checkInterval: Long = _
  private var reporter: Reporter = _

  override def serve(ssc: StreamingContext): BacklogMonitorService = {
    this.ssc = ssc
    conf = ssc.sparkContext.getConf
    threshold = conf.getLong(POTATO_MONITOR_BACKLOG_THRESHOLD_MS_KEY, -1)
    if (threshold < 0) throw new PotatoException(s"Threshold not valid $threshold")
    checkInterval = StreamingUtil.getBatchDuration(ssc).milliseconds
    reporter = new MaxNumReporter(conf.getAll.toMap,
      conf.get(POTATO_MONITOR_BACKLOG_NOTIFY_TYPE_KEY, POTATO_MONITOR_BACKLOG_NOTIFY_TYPE_DEFAULT) match {
        case "ding" => new DingRobotNotify(conf.getAll.toMap)
        case un => throw new PotatoException(s"Unknown notify $un")
      }
    )

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

  override def start(): Unit = {
    logInfo("Start BacklogMonitor.")
    executor.scheduleAtFixedRate(this, checkInterval, checkInterval, TimeUnit.MILLISECONDS)
    logInfo("BacklogMonitor started.")
  }

  override def stop(): Unit = {
    logInfo("Stop BacklogMonitor.")
    executor.shutdownNow()
    logInfo("BacklogMonitor stopped.")
  }

  override def run(): Unit = {
    val current = System.currentTimeMillis()
    val currentDelay = current - lastBatchTime
    if (currentDelay > threshold) {
      logInfo(s"Total delay is upper than threshold, current delay $currentDelay.")
      reporter.problem(copywriting(problem = true, currentDelay))
    } else {
      logInfo(s"Total delay is upper than threshold, current delay $currentDelay.")
      reporter.recover(copywriting(problem = false, currentDelay))
    }
  }

  private val df = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")

  /**
   * 汇报文案。
   */
  private def copywriting(problem: Boolean, currentDelay: Long): String =
    s"""
       |${if (problem) "作业出现积压!" else "作业恢复正常"}
       |reportTime    : ${df.format(new Date())}
       |appName       : ${ssc.sparkContext.appName}
       |appId         : ${ssc.sparkContext.applicationId}
       |webui         : ${ssc.sparkContext.uiWebUrl.getOrElse("unknown")}
       |delayBatch    : ${delayBatch.get()}
       |delayTime     : ${currentDelay / 1000}s
       |threshold     : ${threshold / 1000}s
       |""".stripMargin.trim
}
