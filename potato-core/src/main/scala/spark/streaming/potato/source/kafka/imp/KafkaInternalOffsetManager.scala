package spark.streaming.potato.source.kafka.imp

import spark.streaming.potato.source.kafka.OffsetManager

class KafkaInternalOffsetManager extends OffsetManager{
  override def getOffset: Unit = ???

  override def commitOffset: Unit = ???

  override def commitAll: Unit = ???
}
