package potato.hive

import potato.common.exception.PotatoException

package object exception {

  class PotatoHiveException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

}
