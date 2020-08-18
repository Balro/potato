package potato.spark.sql.writer

import org.apache.spark.sql.DataFrame

/**
 * 将ds格式化为给定格式，便于writer输出。
 */
object DataFrameFormatter {
  def toJSON(df: DataFrame): DataFrame = df.toJSON.toDF("json")

  def toCSV(df: DataFrame, sep: String): DataFrame = df.selectExpr(s"concat_ws('$sep',${df.schema.map(_.name).mkString(",")}) as csv")

  def format(df: DataFrame, options: Map[String, String]): DataFrame = {
    options.get("output") match {
      case Some(output) => DataFrameFormatEnum.withName(output) match {
        case DataFrameFormatEnum.CSV => toCSV(df, options.getOrElse("csv.sep", ","))
        case DataFrameFormatEnum.JSON => toJSON(df)
      }
      case None => df
    }
  }
}

object DataFrameFormatEnum extends Enumeration {
  type DataFrameFormat = Value
  val CSV: DataFrameFormat = Value("csv")
  val JSON: DataFrameFormat = Value("json")
}
