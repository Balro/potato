package spark.streaming.potato.exception

class PotatoException(msg: String = null, throwable: Throwable = null) extends Exception(msg, throwable)

case class ConfigNotFoundException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)
