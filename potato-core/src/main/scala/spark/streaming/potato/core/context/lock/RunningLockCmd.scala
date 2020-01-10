package spark.streaming.potato.core.context.lock

import java.io.FileReader
import java.util.Properties

import spark.streaming.potato.core.conf.PotatoConfKeys._
import spark.streaming.potato.core.utils.GeneralCmd

object RunningLockCmd extends GeneralCmd {
  /**
   * 添加action,argument以及其他初始化。
   */
  override def init(): Unit = {
    addAction("clear", "stop the app by clear the lock. app must monitor its lock status.",
      action = { _ =>
        val properties = new Properties()
        System.getenv("potato_conf_file") match {
          case file: String => properties.load(new FileReader(file))
          case null => throw new Exception("env potato_conf_file not found.")
        }
        val lock = properties.getProperty(
          POTATO_RUNNING_LOCK_TYPE_KEY, POTATO_RUNNING_LOCK_TYPE_DEFAULT
        ) match {
          case "zookeeper" => new ZookeeperRunningLock(null,
            properties.getProperty(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY),
            properties.getProperty(
              POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY, POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT.toString
            ).toInt,
            properties.getProperty(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY),
            properties.getProperty("spark.app.name")
          )
          case lockType => throw new Exception(s"Lock not supported -> $lockType")
        }

        if (lock.clear())
          println("Old lock cleared.")
        println("Old lock not exist.")
      }
    )
  }
}
