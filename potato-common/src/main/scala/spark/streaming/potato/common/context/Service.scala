package spark.streaming.potato.common.context

trait Service {
  def start(): Unit

  def stop(): Unit
}
