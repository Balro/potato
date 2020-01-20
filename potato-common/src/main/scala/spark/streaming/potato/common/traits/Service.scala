package spark.streaming.potato.common.traits

trait Service {
  def start(): Unit

  def stop(): Unit
}
