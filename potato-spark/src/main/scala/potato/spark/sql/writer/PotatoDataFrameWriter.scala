package potato.spark.sql.writer

import java.util.Locale

import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode}
import potato.common.exception.PotatoException
import potato.spark.sql.PotatoDataSource

import scala.collection.mutable

class PotatoDataFrameWriter[T](ds: Dataset[T]) {
  private val options = new mutable.HashMap[String, String]()
  private var mode: SaveMode = SaveMode.ErrorIfExists
  private var source: String = _

  def format(source: String): PotatoDataFrameWriter[T] = {
    this.source = source
    this
  }

  def mode(mode: SaveMode): PotatoDataFrameWriter[T] = {
    this.mode = mode
    this
  }

  def mode(mode: String): PotatoDataFrameWriter[T] = {
    this.mode = mode.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "errorifexists" | "default" => SaveMode.ErrorIfExists
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $mode. " +
        "Accepted save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'.")
    }
    this
  }

  def option(key: String, value: String): PotatoDataFrameWriter[T] = {
    options += (key -> value)
    this
  }

  def options(options: Map[String, String]): PotatoDataFrameWriter[T] = {
    this.options ++= options
    this
  }

  def save(): Unit = {
    val className = PotatoDataSource.sourceMapping.getOrElse(source, throw new PotatoException(s"Source $source not found"))
    val sourceInstance = Class.forName(className).newInstance().asInstanceOf[PotatoDataSource]
    sourceInstance.createWriter(ds.toDF(), mode, options.toMap).write()
  }
}
