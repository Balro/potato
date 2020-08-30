package potato.spark.sql.writer

import java.io.CharArrayWriter

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.json.{PotatoJSONOptions, PotatoJacksonGenerator}
import org.apache.spark.sql.{DataFrame, Encoders, Row}

/**
 * 将ds格式化为给定格式，便于writer输出。
 */
object DataFrameFormatter {
  /**
   * 使用默认df.toJSON方法。
   */
  def toJSONRaw(df: DataFrame): DataFrame = df.toJSON.toDF("json")

  /**
   * 修改过的df.toJSON方法，可以写入null字段。
   */
  def toJSON(df: DataFrame): DataFrame = {
    val rowSchema = df.schema
    val sessionLocalTimeZone = df.sparkSession.sessionState.conf.sessionLocalTimeZone
    df.mapPartitions { iter =>
      val writer = new CharArrayWriter()
      // create the Generator without separator inserted between 2 records
      val gen = new PotatoJacksonGenerator(rowSchema, writer,
        new PotatoJSONOptions(Map.empty[String, String], sessionLocalTimeZone))
      val field = classOf[DataFrame].getDeclaredField("exprEnc")
      field.setAccessible(true)

      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): String = {
          gen.write(field.get(df).asInstanceOf[ExpressionEncoder[Row]].toRow(iter.next()))
          gen.flush()

          val json = writer.toString
          if (hasNext) {
            writer.reset()
          } else {
            gen.close()
          }

          json
        }
      }
    }(Encoders.STRING).toDF("json")
  }

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
