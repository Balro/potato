package potato.spark.monitor.streaming

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchSubmitted}
import potato.common.exception.ConfigException
import potato.common.sender.{DingDingSender, Sender}
import potato.spark.conf._
import potato.spark.service.StreamingService

class StreamingBacklogMonitorService extends StreamingService with StreamingListener {
  private var ssc: StreamingContext = _

  private def conf: SparkConf = ssc.sparkContext.getConf

  private var threshold: Long = _
  private var sender: Sender[String, Unit] = _
  private var reportInterval: Long = _
  private var reportMax: Int = _

  /**
   * 初始化服务。
   */
  override def serve(ssc: StreamingContext): StreamingService = {
    this.ssc = ssc
    threshold = conf.get(POTATO_MONITOR_STREAMING_BACKLOG_THRESHOLD_MS_KEY).toLong
    sender = conf.get(POTATO_MONITOR_STREAMING_BACKLOG_REPORT_SENDER_KEY, POTATO_MONITOR_STREAMING_BACKLOG_REPORT_SENDER_DEFAULT) match {
      case "ding" => new DingDingSender(conf)
      case other => throw ConfigException(s"Sender type $other not supported.")
    }
    reportInterval = conf.get(POTATO_MONITOR_STREAMING_BACKLOG_REPORT_INTERVAL_MS_KEY, POTATO_MONITOR_STREAMING_BACKLOG_REPORT_INTERVAL_MS_DEFAULT).toLong
    reportMax = conf.get(POTATO_MONITOR_STREAMING_BACKLOG_REPORT_MAX_KEY, POTATO_MONITOR_STREAMING_BACKLOG_REPORT_MAX_DEFAULT).toInt
    ssc.addStreamingListener(this)
    this
  }

  override val serviceName: String = "StreamingBacklogMonitor"
  private val running: AtomicBoolean = new AtomicBoolean(false)

  /**
   * 启动服务方法，禁止外部调用，启动服务请使用[[checkAndStart]]。
   */
  override protected def start(): Unit = running.set(true)

  /**
   * 停止服务方法，禁止外部调用，停止服务请使用[[checkAndStop]]。
   */
  override protected def stop(): Unit = running.set(false)

  private val delayBatch: AtomicLong = new AtomicLong(0)
  private val isReported: AtomicBoolean = new AtomicBoolean(false)
  private var lastReportTime: Long = 0
  private val reportNum: AtomicInteger = new AtomicInteger(0)

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
    delayBatch.decrementAndGet()
    val current = batchCompleted.batchInfo.processingEndTime.get
    val currentDelay = batchCompleted.batchInfo.totalDelay.get
    if (currentDelay > threshold) {
      if (!isReported.get() || (reportNum.get() < reportMax && current - lastReportTime > reportInterval)) {
        sender.send(mkSenderString(problem = true, currentDelay))
        lastReportTime = current
        reportNum.incrementAndGet()
        isReported.set(true)
        logWarning(s"Streaming job delayed, current delay time $currentDelay, sending delay message [${reportNum.get()}/$reportMax].")
      }
    } else {
      if (isReported.get()) {
        sender.send(mkSenderString(problem = false, currentDelay))
        isReported.set(false)
        reportNum.set(0)
        logInfo(s"Streaming job is working in time.")
      }
    }
  }

  private val df = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")

  /**
   * 汇报内容文案。
   */
  private def mkSenderString(problem: Boolean, currentDelay: Long): String =
    s"""
       |${if (problem) "作业出现积压!" else "作业恢复正常"}
       |上报时间 : ${df.format(new Date())}
       |作业名称 : ${ssc.sparkContext.appName}
       |作业id   : ${ssc.sparkContext.applicationId}
       |作业地址 : ${ssc.sparkContext.uiWebUrl.getOrElse("unknown")}
       |延迟批次 : ${delayBatch.get()}
       |延迟时间 : ${currentDelay / 1000}s
       |上报阈值 : ${threshold / 1000}s
       |上报次数 : [${reportNum.get()}/$reportMax]
       |""".stripMargin.trim
}
