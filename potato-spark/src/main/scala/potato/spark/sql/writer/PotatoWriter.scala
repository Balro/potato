package potato.spark.sql.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

trait PotatoWriter {
  def option(options: Map[String, String]): PotatoWriter

  def mode(mode: SaveMode): PotatoWriter

  def save(ds: DataFrame): Unit
}
