package spark.potato.lock

import spark.potato.common.exception.PotatoException

package object exception {

  class LockException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

  case class LockNotSupportedException(msg: String = null, throwable: Throwable = null) extends LockException(msg, throwable)

  case class CannotGetSingletonLockException(msg: String = null, throwable: Throwable = null) extends LockException

  case class LockMismatchException(msg: String = null, throwable: Throwable = null) extends LockException

}
