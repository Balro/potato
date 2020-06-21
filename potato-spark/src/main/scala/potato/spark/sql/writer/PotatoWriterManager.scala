package potato.spark.sql.writer

import org.apache.spark.sql.{DataFrame, SaveMode}
import potato.common.exception.PotatoException

import scala.collection.mutable

class PotatoWriterManager(df: DataFrame) {
  private var writerClass: String = _
  private val options = new mutable.HashMap[String, String]()
  private var mode = SaveMode.ErrorIfExists

  def format(format: String): PotatoWriterManager = {
    this.writerClass = writerMapping(format)
    this
  }

  def writer(clazz: String): PotatoWriterManager = {
    this.writerClass = clazz
    this
  }

  def mode(mode: SaveMode): PotatoWriterManager = {
    this.mode = mode
    this
  }

  def option(key: String, value: String): PotatoWriterManager = {
    options += (key -> value)
    this
  }

  def options(options: Map[String, String]): PotatoWriterManager = {
    this.options ++= options
    this
  }

  def save(): Unit = {
    val writer = Class.forName(writerClass) match {
      case clazz: Class[_] if classOf[PotatoWriter].isAssignableFrom(clazz) => clazz.newInstance().asInstanceOf[PotatoWriter]
      case unknown => throw new PotatoException(s"Unknown potato writer $unknown.")
    }
    writer.mode(mode).option(options.toMap).save(df)
  }
}
