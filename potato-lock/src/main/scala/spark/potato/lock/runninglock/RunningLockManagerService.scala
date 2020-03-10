package spark.potato.lock.runninglock

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.fasterxml.jackson.core.JsonParseException
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import spark.potato.common.exception.PotatoException
import spark.potato.common.service.ContextService
import spark.potato.common.tools.DaemonThreadFactory
import spark.potato.lock.conf._
import spark.potato.lock.exception.{CannotGetRunningLockException, LockMismatchException}


/**
 * running lock管理工具。
 * running lock为了解决作业重复启动的问题，当已有作业获取锁时，新作业无法再次提交。
 * 或者新作业可以直接停止旧作业，代替旧作业运行。
 */
class RunningLockManagerService extends ContextService with Logging {
  implicit val formats: Formats = DefaultFormats
  private var sc: SparkContext = _
  private var conf: SparkConf = _
  private[runninglock] var lock: RunningLock = _

  override def serve(sc: SparkContext): RunningLockManagerService = {
    this.sc = sc
    conf = sc.getConf
    lock = conf.get(
      POTATO_RUNNING_LOCK_TYPE_KEY, POTATO_RUNNING_LOCK_TYPE_DEFAULT
    ) match {
      case "zookeeper" => new ZookeeperRunningLock(
        this,
        conf.get(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY),
        conf.getInt(POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT),
        conf.get(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_DEFAULT),
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
  def tryLock(maxTry: Int = conf.getInt(POTATO_RUNNING_LOCK_TRY_MAX_KEY, POTATO_RUNNING_LOCK_TRY_MAX_DEFAULT),
              interval: Long = conf.getLong(POTATO_RUNNING_LOCK_TRY_INTERVAL_MS_KEY, POTATO_RUNNING_LOCK_TRY_INTERVAL_MS_DEFAULT),
              force: Boolean = conf.getBoolean(POTATO_RUNNING_LOCK_FORCE_KEY, POTATO_RUNNING_LOCK_FORCE_DEFAULT)
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
    sc.stop()
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
    val timeout = conf.getLong(POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT)
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
      conf.getLong(POTATO_RUNNING_LOCK_HEARTBEAT_INTERVAL_MS_KEY, POTATO_RUNNING_LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT),
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

  private val started = new AtomicBoolean(false)

  override def start(): Unit = {
    if (started.get()) {
      logWarning("Service RunningLockManager already started.")
      return
    }
    logInfo("Start RunningLockManager.")
    startHeartbeat()
    logInfo("RunningLockManager started.")
  }
}


