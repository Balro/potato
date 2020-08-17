package potato.kafka010.writer

object KafkaWriteFormat extends Enumeration {
  type KafkaWriteFormat = Value
  val FIRST: KafkaWriteFormat = Value("first")
  val CSV: KafkaWriteFormat = Value("csv")
  val JSON: KafkaWriteFormat = Value("json")
}
