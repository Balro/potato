package potato.hbase

import spark.potato.common.exception.PotatoException

package object exception {

  case class TableClosedException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

}
