package potato.kafka010

import potato.common.exception.PotatoException

package object exception {

  class PotatoKafkaException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

  case class NotCacheAnyOffsetsException(msg: String = null, throwable: Throwable = null) extends PotatoKafkaException(msg, throwable)

  /**
   * 扩展KafkaException，用于指示未找到元数据的异常。
   */
  case class MetadataNotFoundException(msg: String = null, throwable: Throwable = null) extends PotatoKafkaException(msg, throwable)

  case class InvalidConfigException(msg: String = null, throwable: Throwable = null) extends PotatoKafkaException(msg, throwable)

  case class SinkFailedException(msg: String = null, throwable: Throwable = null) extends PotatoKafkaException(msg, throwable)

}