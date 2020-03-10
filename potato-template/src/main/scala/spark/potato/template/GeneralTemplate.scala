package spark.potato.template

abstract class GeneralTemplate {
  def init(): Unit

  def doWork(): Unit

  def clear(): Unit
}
