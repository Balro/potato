package spark.potato.lock.running

import org.apache.spark.internal.Logging
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException, SessionExpiredException}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

trait RunningLock {
  /**
   * 加锁，并附加状态信息。
   *
   * @return 是否成功加锁。
   */
  def lock(msg: String): Boolean

  /**
   * 释放锁。
   */
  def release(): Unit

  /**
   * 清理旧锁。
   *
   * @return 旧锁存在返回true，旧锁不存在返回false。
   */
  def clear(): Boolean

  /**
   * 获取锁的状态信息。
   *
   * @return (是否已加锁，当前锁信息)
   */
  def getLock: (Boolean, String)

  /**
   * 更新锁的状态信息。
   */
  def setMsg(msg: String): Unit
}

/**
 * RunningLock的zookeeper实现。
 *
 * @param manager 用于在锁异常时对manager进行反馈。
 * @param quorum  zookeeper地址。
 * @param timeout zookeeper连接超时时间。
 * @param path    锁路径。
 * @param appName 作业名。
 */
class ZookeeperRunningLock(manager: RunningLockManager, quorum: String, timeout: Int, path: String, appName: String) extends RunningLock
  with Watcher with Logging {
  val zookeeper = new ZooKeeper(quorum, timeout, this)
  val lockPath: String = path + "/" + appName + ".lock"
  checkPath(path)

  /**
   * 初始化锁路径，如锁路径不存在，则自动创建。
   */
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

  /**
   * Watcher的空实现。
   */
  override def process(event: WatchedEvent): Unit = {}

  /**
   * 监听锁节点delete事件，用于通过监听锁状态来实现作业管理，当锁节点被删除时停止作业。
   */
  case class LockDeleteWatcher() extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getType == Watcher.Event.EventType.NodeDeleted) {
        logError("Lock has been deleted, stop app.")
        manager.release()
        return
      } else if (event.getState == Watcher.Event.KeeperState.Expired) {
        logError("Lock has been expired, stop app.")
        manager.release()
        return
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
