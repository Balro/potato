package potato.spark.sql.writer

import org.apache.spark.sql.{Dataset, SparkSession}

object TmpTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._
    val ds: Dataset[String] = spark.createDataset(Seq("a", "b"))
    val d: PotatoDataFrameWriter[String] = ds.potatoWrite
  }
}
