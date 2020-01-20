package spark.streaming.potato.plugins.lock

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException, SessionExpiredException}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}
import org.json.{JSONException, JSONObject}
import spark.streaming.potato.common.exception.PotatoException
import spark.streaming.potato.common.traits.Service
import spark.streaming.potato.common.utils.DaemonThreadFactory
import spark.streaming.potato.plugins.lock.LockConfigKeys._

import scala.collection.JavaConversions

class RunningLockManager(private[lock] val ssc: StreamingContext) extends Service with Logging {
  val conf: SparkConf = ssc.sparkContext.getConf
  val lock: RunningLock = conf.get(
    POTATO_RUNNING_LOCK_TYPE_KEY, POTATO_RUNNING_LOCK_TYPE_DEFAULT
  ) match {
    case "zookeeper" => new ZookeeperRunningLock(
      this,
      conf.get(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY),
      conf.getInt(POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT),
      conf.get(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY),
      ssc.sparkContext.appName
    )
    case t => throw new PotatoException(s"Running lock type -> $t not supported.")
  }

  var locked: Boolean = false
  var executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)

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
    ssc.stop()
    if (!executor.awaitTermination(5, TimeUnit.SECONDS))
      executor.shutdownNow()
    lock.release()
    locked = false
  }

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
      conf.getLong(POTATO_RUNNING_LOCK_HEARTBEAT_INTERVAL_MS_KEY, POTATO_RUNNING_LOCK_TRY_INTERVAL_MS_DEFAULT),
      TimeUnit.MILLISECONDS)
  }

  def heartbeat(): Unit = {
    if (!locked) {
      logWarning(s"Skip this heartbeat because of we have not locked yet.")
      return
    }

    var oldMsg = null.asInstanceOf[String]
    try {
      val (isLocked, msg) = {
        val (l, m) = lock.getLock
        oldMsg = m
        l -> new JSONObject(m)
      }

      val oldAppName = msg.get("appName")
      val oldApplicationId = msg.get("applicationId")
      val curAppName = ssc.sparkContext.appName
      val curApplicationId = ssc.sparkContext.applicationId

      if (isLocked && oldAppName == curAppName && oldApplicationId == curApplicationId) {
        lock.setMsg(createMsg)
        return
      }
    } catch {
      case e: JSONException => logWarning(s"Oldmsg is not valid -> $oldMsg", e)
    }
    throw LockMismatchException(s"Lock mismatch, current: $createMsg -> old: $oldMsg")
  }

  /*
  appName
  applicationId
  applicationAttemptId
  deployMode
  lastHeartbeatTime
  master
  startTime
  user
  webUri
   */
  def createMsg: String = {
    val ctx = ssc.sparkContext
    new JSONObject(JavaConversions.mapAsJavaMap(Map(
      "appName" -> ctx.appName.toString,
      "applicationId" -> ctx.applicationId.toString,
      "applicationAttemptId" -> ctx.applicationAttemptId.getOrElse("-1"),
      "deployMode" -> ctx.deployMode,
      "lastHeartbeatTime" -> System.currentTimeMillis.toString,
      "master" -> ctx.master,
      "startTime" -> ctx.startTime.toString,
      "user" -> ctx.sparkUser,
      "webUri" -> ctx.uiWebUrl.getOrElse("null")
    ))).toString
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

trait RunningLock {
  def lock(msg: String): Boolean

  def release(): Unit

  def clear(): Boolean

  def getLock: (Boolean, String)

  def setMsg(msg: String): Unit
}

class ZookeeperRunningLock(manager: RunningLockManager, addr: String, timeout: Int, path: String, appName: String) extends RunningLock
  with Watcher with Logging {
  val zookeeper = new ZooKeeper(addr, timeout, this)
  val lockPath: String = path + "/" + appName + ".lock"
  checkPath(path)

  def checkPath(path: String): Unit = {
    if (path.nonEmpty && zookeeper.exists(path, false) == null) {
      checkPath(path.substring(0, path.lastIndexOf("/")))
      zookeeper.create(path, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }
  }

  override def lock(msg: String): Boolean = {
    try {
      zookeeper.create(lockPath, msg.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      zookeeper.exists(lockPath, LockDeleteWatcher())
      logInfo("Lock successfully.")
      true
    } catch {
      case e: NodeExistsException =>
        logWarning("Old lock exists.", e)
        false
    }
  }

  override def release(): Unit = {
    logInfo("Release lock.")
    zookeeper.close()
  }

  override def clear(): Boolean = {
    try {
      zookeeper.delete(lockPath, -1)
    } catch {
      case _: NoNodeException =>
        logWarning("Old lock not found.")
        return false
      case e: Throwable => throw e
    }
    logInfo("Old lock cleared.")
    true
  }

  override def getLock: (Boolean, String) = {
    try {
      true -> new String(zookeeper.getData(lockPath, false, null))
    } catch {
      case _: NoNodeException => false -> ""
    }
  }

  override def setMsg(msg: String): Unit = {
    zookeeper.setData(lockPath, msg.getBytes(), -1)
  }

  override def process(event: WatchedEvent): Unit = {}

  case class LockDeleteWatcher() extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getType == Watcher.Event.EventType.NodeDeleted) {
        logError("Lock has been deleted, stop app.")
        manager.release()
      } else if (event.getState == Watcher.Event.KeeperState.Expired) {
        logError("Lock has been expired, stop app.")
        manager.release()
      }

      var registered = false
      while (!registered) {
        try {
          val state = zookeeper.exists(lockPath, this)
          if (state == null) {
            logError("Lock has lost, stop app.")
            manager.release()
          }
          registered = true
        } catch {
          case e: SessionExpiredException =>
            logWarning("Session expired, stop app.", e)
            manager.release()
            return
          case e: Throwable =>
            logWarning("Lock delete watcher meet err, try re register.", e)
        }
      }
    }
  }

}
