package potato.spark.exception

import potato.common.exception.PotatoException

case class PotatoStreamingException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

case class SparkContextNotInitializedException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

case class UnknownServiceException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

case class ServiceAlreadyRegisteredException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

class LockException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

case class LockNotSupportedException(msg: String = null, throwable: Throwable = null) extends LockException(msg, throwable)

case class CannotGetSingletonLockException(msg: String = null, throwable: Throwable = null) extends LockException

case class LockMismatchException(msg: String = null, throwable: Throwable = null) extends LockException