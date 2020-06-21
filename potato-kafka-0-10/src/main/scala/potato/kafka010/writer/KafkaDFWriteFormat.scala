package potato.kafka010.writer

object KafkaDFWriteFormat extends Enumeration {
  type KafkaDFWriteFormat = Value
  val FIRST: KafkaDFWriteFormat = Value("first")
  val CSV: KafkaDFWriteFormat = Value("csv")
  val JSON: KafkaDFWriteFormat = Value("json")
}
