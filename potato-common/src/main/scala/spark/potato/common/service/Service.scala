package spark.potato.common.service

/**
 * service特质，用于创建附加服务。
 */
trait Service {
  def start(): Unit

  def stop(): Unit
}

//case class ServiceInfo(key: String, default: Boolean, clazz: String)
