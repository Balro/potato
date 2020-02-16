package spark.streaming.potato.common.util

trait Service {
  def start(): Unit

  def stop(): Unit
}
