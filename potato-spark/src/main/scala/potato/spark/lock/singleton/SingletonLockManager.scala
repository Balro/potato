package potato.spark.lock.singleton

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import potato.spark.conf._
import potato.spark.exception._
import potato.spark.lock.LockMessage

/**
 * singleton lock 代理工具。
 * 使用 singleton lock 为了解决作业重复启动的问题，当已有作业获取锁时，新作业无法再次提交。
 * 或者新作业可以直接停止旧作业，代替旧作业运行。
 */
class SingletonLockManager(lockService: SingletonLockService) extends Logging {
  private[singleton] var lock: SingletonLock = {
    lockService.conf.get(POTATO_LOCK_SINGLETON_TYPE_KEY, POTATO_LOCK_SINGLETON_TYPE_DEFAULT) match {
      case "zookeeper" => new ZookeeperSingletonLock(lockService,
        lockService.conf.get(POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY),
        lockService.conf.get(POTATO_LOCK_SINGLETON_ZOOKEEPER_TIMEOUT_KEY, POTATO_LOCK_SINGLETON_ZOOKEEPER_TIMEOUT_DEFAULT).toInt,
        lockService.conf.get(POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY, POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT),
        lockService.sc.appName
      )
      case other => throw LockNotSupportedException(other)
    }
  }

  /**
   * 重试给定方法并获取返回值。
   *
   * @param interval 重试间隔，单位毫秒。
   */
  def withRetry[T](max: Int, interval: Long)(f: () => T): Either[Exception, T] = {
    var ex: Exception = null
    1.to(max).foreach { i =>
      try {
        return Right(f())
      } catch {
        case e: Exception =>
          if (i < max) {
            logWarning(s"Try $i/$max failed, sleep ${interval}ms and wait next try.", e)
            TimeUnit.MILLISECONDS.sleep(interval)
          } else {
            logWarning(s"Try $i/$max failed, return with exception.", e)
            ex = e
          }
      }
    }
    Left(ex)
  }

  /**
   * 尝试对作业加锁，如果锁已存在，则进行重试，重试次数耗尽时停止尝试加锁的作业。
   * 如果进行强制加锁，则自动清理旧锁后再加新锁。
   *
   * @param maxTry   尝试加锁最大重试次数。
   * @param interval 尝试加锁重试间隔。
   * @param force    是否强制加锁。
   */
  def tryLock(maxTry: Int = lockService.conf.getInt(POTATO_LOCK_SINGLETON_TRY_MAX_KEY, POTATO_LOCK_SINGLETON_TRY_MAX_DEFAULT.toInt),
              interval: Long = lockService.conf.getLong(POTATO_LOCK_SINGLETON_TRY_INTERVAL_MS_KEY, POTATO_LOCK_SINGLETON_TRY_INTERVAL_MS_DEFAULT.toLong),
              force: Boolean = lockService.conf.getBoolean(POTATO_LOCK_SINGLETON_FORCE_KEY, POTATO_LOCK_SINGLETON_FORCE_DEFAULT.toBoolean)
             ): Unit = {
    this.synchronized {
      withRetry(maxTry, interval) { () =>
        if (lock.tryLock(new LockMessage(lockService.sc).toJsonString)) {
          logInfo("Get lock successfully.")
          return
        } else if (force) {
          logWarning(s"Get lock failed, try clean old lock ${lock.getMsg._2}.")
          lock.clean()
          if (!lock.tryLock(new LockMessage(lockService.sc).toJsonString)) {
            throw CannotGetSingletonLockException("Force get lock failed.")
          }
        } else {
          throw CannotGetSingletonLockException("Force get lock failed.")
        }
      } match {
        case Left(e) => throw e
        case _ =>
      }
    }
  }

  def release(): Unit = lock.release()
}
