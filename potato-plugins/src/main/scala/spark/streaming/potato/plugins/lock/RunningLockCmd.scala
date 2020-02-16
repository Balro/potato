package spark.streaming.potato.plugins.lock

import org.apache.spark.SparkConf
import spark.streaming.potato.common.util.GeneralCmd
import spark.streaming.potato.plugins.lock.LockConfigKeys._

object RunningLockCmd extends GeneralCmd {
  /**
   * 添加action,argument以及其他初始化。
   */
  override def init(): Unit = {
    addAction("clear", "stop the app by clear the lock. app must monitor its lock status.",
      action = { () =>
        val conf = new SparkConf()
        val lock = conf.get(
          POTATO_RUNNING_LOCK_TYPE_KEY, POTATO_RUNNING_LOCK_TYPE_DEFAULT
        ) match {
          case "zookeeper" => new ZookeeperRunningLock(null,
            conf.get(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY),
            conf.getInt(
              POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT
            ),
            conf.get(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY),
            conf.get("spark.app.name")
          )
          case lockType => throw new Exception(s"Lock not supported -> $lockType")
        }

        output(lock.getLock)
        if (lock.clear())
          output("Old lock cleared.")
        else
          output("Old lock does not exist.")
        lock.release()
      }
    )

    addAction("state", describe = "show current lock msg.",
      action = { () =>
        val conf = new SparkConf()
        val lock = conf.get(
          POTATO_RUNNING_LOCK_TYPE_KEY, POTATO_RUNNING_LOCK_TYPE_DEFAULT
        ) match {
          case "zookeeper" => new ZookeeperRunningLock(null,
            conf.get(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY),
            conf.getInt(
              POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT
            ),
            conf.get(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY),
            conf.get("spark.app.name")
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
