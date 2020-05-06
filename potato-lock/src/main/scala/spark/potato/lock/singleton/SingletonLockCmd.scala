package spark.potato.lock.singleton

import org.apache.spark.SparkConf
import spark.potato.common.cmd.ActionCMDBase
import spark.potato.common.conf._
import spark.potato.lock.conf._

/**
 * SingletonLock命令行管理工具。
 */
object SingletonLockCmd extends ActionCMDBase {
  /**
   * 添加action,argument以及其他初始化。
   */
  override def init(): Unit = {
    // 清楚已存在的锁。
    addAction("clear", "stop the app by clear the lock. app must monitor its lock status.",
      action = { () =>
        val conf = new SparkConf()
        val lock = conf.get(
          POTATO_LOCK_SINGLETON_TYPE_KEY, POTATO_LOCK_SINGLETON_TYPE_DEFAULT
        ) match {
          case "zookeeper" => new ZookeeperSingletonLock(null,
            conf.get(POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY),
            conf.getInt(
              POTATO_LOCK_SINGLETON_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_LOCK_SINGLETON_HEARTBEAT_TIMEOUT_MS_DEFAULT
            ),
            conf.get(POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY, POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT),
            conf.get(POTATO_APP_NAME_KEY)
          )
          case lockType => throw new Exception(s"Lock not supported -> $lockType")
        }

        output(lock.getLock)
        if (lock.clear())
          output("\nOld lock cleared.")
        else
          output("Old lock does not exist.")
        lock.release()
      }
    )

    // 查看当前锁状态。
    addAction("state", describe = "show current lock msg.",
      action = { () =>
        val conf = new SparkConf()
        val lock = conf.get(
          POTATO_LOCK_SINGLETON_TYPE_KEY, POTATO_LOCK_SINGLETON_TYPE_DEFAULT
        ) match {
          case "zookeeper" => new ZookeeperSingletonLock(null,
            conf.get(POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY),
            conf.getInt(
              POTATO_LOCK_SINGLETON_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_LOCK_SINGLETON_HEARTBEAT_TIMEOUT_MS_DEFAULT
            ),
            conf.get(POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY, POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT),
            conf.get(POTATO_APP_NAME_KEY)
          )
          case lockType => throw new Exception(s"Lock not supported -> $lockType")
        }

        lock.getLock match {
          case (true, msg) => output(msg)
          case (false, _) => output("Lock not found.")
        }

        lock.release()
      })
  }
}
