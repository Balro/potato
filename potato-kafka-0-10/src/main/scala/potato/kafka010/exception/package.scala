package potato.kafka010

import org.apache.kafka.common.KafkaException

package object exception {

  case class NotCacheAnyOffsetsException(msg: String = null, throwable: Throwable = null) extends KafkaException(msg, throwable)

  /**
   * 扩展KafkaException，用于指示未找到元数据的异常。
   */
  case class MetadataNotFoundException(msg: String = null, throwable: Throwable = null) extends KafkaException(msg, throwable)

  case class InvalidConfigException(msg: String = null, throwable: Throwable = null) extends KafkaException(msg, throwable)

  case class SinkFailedException(msg: String = null, throwable: Throwable = null) extends KafkaException(msg, throwable)

}