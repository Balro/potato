package spark.potato.common.service

trait ServiceTrait {
  def start(): Unit

  def stop(): Unit
}

case class ServiceInfo(key: String, default: Boolean, clazz: String)
