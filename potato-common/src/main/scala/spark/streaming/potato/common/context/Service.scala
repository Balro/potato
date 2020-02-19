package spark.streaming.potato.common.context

trait Service {
  def start(): Unit

  def stop(): Unit
}

case class ServiceInfo(key: String, default: Boolean, clazz: String)
