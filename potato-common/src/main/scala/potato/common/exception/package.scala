package potato.common.exception

class PotatoException(msg: String = null, throwable: Throwable = null) extends Exception(msg, throwable)

class PotatoConfException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

class PotatoCmdException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

class PotatoMethodException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)
