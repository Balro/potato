package spark.potato.lock

import spark.potato.common.exception.PotatoException

class LockException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

case class CannotGetRunningLockException(msg: String = null, throwable: Throwable = null) extends LockException

case class LockMismatchException(msg: String = null, throwable: Throwable = null) extends LockException