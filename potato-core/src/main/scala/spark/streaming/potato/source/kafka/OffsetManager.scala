package spark.streaming.potato.source.kafka

trait OffsetManager {
  def getOffset

  def commitOffset

  def commitAll
}
