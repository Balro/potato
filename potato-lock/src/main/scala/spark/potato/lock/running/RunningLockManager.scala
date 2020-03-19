package spark.potato.lock.running

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.fasterxml.jackson.core.JsonParseException
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spark.potato.common.exception.PotatoException
import spark.potato.common.service.{ContextService, Service, StreamingService}
import spark.potato.common.tools.DaemonThreadFactory
import spark.potato.lock.conf._
import spark.potato.lock.exception.{CannotGetRunningLockException, LockMismatchException}

/**
 * 区分StreamingContext和SparkContext，避免停止了SparkContext而未停止StreamingContext导致报错。
 */
class StreamingRunningLockService extends RunningLockManager with StreamingService with Logging {
  override val serviceName: String = POTATO_LOCK_RUNNING_STREAMING_SERVICE_NAME

  private var ssc: StreamingContext = _

  override def stopSpark(): Unit = ssc.stop()

  /**
   * 初始化服务。
   */
  override def serve(ssc: StreamingContext): StreamingService = {
    this.ssc = ssc
    this.sc = ssc.sparkContext
    init()
    this
  }
}

/**
 * 使用于SparkContext，不可用于StreamingContext，否则在yarn模式下降导致StreamingContext报错而意外重启。
 */
class ContextRunningLockService extends RunningLockManager with ContextService with Logging {
  override val serviceName: String = POTATO_LOCK_RUNNING_CONTEXT_SERVICE_NAME

  override def stopSpark(): Unit = sc.stop()

  /**
   * 初始化服务。
   */
  override def serve(sc: SparkContext): ContextRunningLockService = {
    this.sc = sc
    init()
    this
  }
}

/**
 * running lock管理工具。
 * running lock为了解决作业重复启动的问题，当已有作业获取锁时，新作业无法再次提交。
 * 或者新作业可以直接停止旧作业，代替旧作业运行。
 */
abstract class RunningLockManager extends Service with Logging {
  implicit val formats: Formats = DefaultFormats
  protected var sc: SparkContext = _

  private def conf = sc.getConf

  private[running] var lock: RunningLock = _

  def stopSpark(): Unit

  def init(): RunningLockManager = {
    lock = conf.get(
      POTATO_LOCK_RUNNING_TYPE_KEY, POTATO_LOCK_RUNNING_TYPE_DEFAULT
    ) match {
      case "zookeeper" => new ZookeeperRunningLock(
        this,
        conf.get(POTATO_LOCK_RUNNING_ZOOKEEPER_QUORUM_KEY),
        conf.getInt(POTATO_LOCK_RUNNING_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_LOCK_RUNNING_HEARTBEAT_TIMEOUT_MS_DEFAULT),
        conf.get(POTATO_LOCK_RUNNING_ZOOKEEPER_PATH_KEY, POTATO_LOCK_RUNNING_ZOOKEEPER_PATH_DEFAULT),
        sc.appName
      )
      case t => throw new PotatoException(s"Running lock type -> $t not supported.")
    }
    this
  }

  private var locked: Boolean = false

  def isLocked: Boolean = locked

  private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)

  /**
   * 尝试对作业加锁，如果锁已存在，则进行重试，重试次数耗尽时停止尝试加锁的作业。
   * 如果进行强制加锁，则自动清理旧锁后再加新锁。
   *
   * @param maxTry   尝试加锁最大重试次数。
   * @param interval 尝试加锁重试间隔。
   * @param force    是否强制加锁。
   */
  def tryLock(maxTry: Int = conf.getInt(POTATO_LOCK_RUNNING_TRY_MAX_KEY, POTATO_LOCK_RUNNING_TRY_MAX_DEFAULT),
              interval: Long = conf.getLong(POTATO_LOCK_RUNNING_TRY_INTERVAL_MS_KEY, POTATO_LOCK_RUNNING_TRY_INTERVAL_MS_DEFAULT),
              force: Boolean = conf.getBoolean(POTATO_LOCK_RUNNING_FORCE_KEY, POTATO_LOCK_RUNNING_FORCE_DEFAULT)
             ): Unit = {
    this.synchronized {
      var tried = 0
      while (tried < maxTry) {
        locked = lock.lock(createMsg)
        if (locked) {
          logInfo("Get lock successfully.")
          return
        } else if (force) {
          logWarning("Get lock failed, try force get lock.")
          lock.clear()
          tryLock(maxTry, interval, force = false)
          return
        } else {
          TimeUnit.MILLISECONDS.sleep(interval)
        }
        tried += 1
      }
      throw CannotGetRunningLockException(s"Current lock -> ${lock.getLock}")
    }
  }

  def release(): Unit = {
    stopSpark()
    executor.shutdown()
    if (!executor.awaitTermination(5, TimeUnit.SECONDS))
      executor.shutdownNow()
    lock.release()
    locked = false
  }

  /**
   * 启动心跳程序，定时向锁汇报作业信息。同时检查当前时间与上次汇报时间是否超时，如超时则释放锁。
   */
  def startHeartbeat(): Unit = {
    if (!locked) tryLock()
    val timeout = conf.getLong(POTATO_LOCK_RUNNING_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_LOCK_RUNNING_HEARTBEAT_TIMEOUT_MS_DEFAULT)
    var lastHeartbeat = System.currentTimeMillis()
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        if (System.currentTimeMillis() - lastHeartbeat > timeout) {
          logError("Heartbeat timeout, stop app.")
          release()
        }

        try {
          heartbeat()
          lastHeartbeat = System.currentTimeMillis()
        } catch {
          case e: LockMismatchException =>
            logError(s"Lock mismatch, stop ssc.", e)
            release()
          case e: Throwable =>
            logWarning("Heartbeat meet exception.", e)
        }
      }
    }, 0,
      conf.getLong(POTATO_LOCK_RUNNING_HEARTBEAT_INTERVAL_MS_KEY, POTATO_LOCK_RUNNING_HEARTBEAT_INTERVAL_MS_DEFAULT),
      TimeUnit.MILLISECONDS)
  }

  /**
   * 进行一次心跳。
   */
  def heartbeat(): Unit = {
    // 如锁还未获取，则调过此次心跳。
    if (!locked) {
      logWarning(s"Skip this heartbeat because of we have not locked yet.")
      return
    }

    var oldMsg = null.asInstanceOf[String]
    try {
      val (isLocked, msg) = {
        val (l, m) = lock.getLock
        oldMsg = m
        l -> parse(m)
      }

      val oldAppName = msg.\("appName").extract[String]
      val oldApplicationId = msg.\("applicationId").extract[String]
      val curAppName = sc.appName
      val curApplicationId = sc.applicationId

      if (isLocked && oldAppName == curAppName && oldApplicationId == curApplicationId) {
        lock.setMsg(createMsg)
        return
      }
    } catch {
      case e: JsonParseException => logWarning(s"Oldmsg is not valid -> $oldMsg", e)
    }
    // 如现有锁与当前作业信息不匹配，则抛出异常。
    throw LockMismatchException(s"Lock mismatch, current: $createMsg -> old: $oldMsg")
  }

  /**
   * 锁携带信息包括:
   * appName
   * applicationId
   * applicationAttemptId
   * deployMode
   * lastHeartbeatTime
   * master
   * startTime
   * user
   * webUri
   */
  def createMsg: String = {
    compact(Map(
      "appName" -> sc.appName.toString,
      "applicationId" -> sc.applicationId.toString,
      "applicationAttemptId" -> sc.applicationAttemptId.getOrElse("-1"),
      "deployMode" -> sc.deployMode,
      "lastHeartbeatTime" -> System.currentTimeMillis.toString,
      "master" -> sc.master,
      "startTime" -> sc.startTime.toString,
      "user" -> sc.sparkUser,
      "webUri" -> sc.uiWebUrl.getOrElse("null")
    ))
  }

  override def stop(): Unit = {
    logInfo("Stop RunningLockManager.")
    release()
    logInfo("RunningLockManager stopped.")
  }

  override def start(): Unit = {
    logInfo("Start RunningLockManager.")
    startHeartbeat()
    logInfo("RunningLockManager started.")
  }
}


