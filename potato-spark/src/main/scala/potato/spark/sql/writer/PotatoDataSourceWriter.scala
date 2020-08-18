package potato.spark.sql.writer

trait PotatoDataSourceWriter extends Serializable {
  def write(): Unit
}
