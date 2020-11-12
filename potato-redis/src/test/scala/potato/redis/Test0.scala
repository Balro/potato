package potato.redis

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import potato.spark.sql.writer._

object Test0 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._

    val df = spark.read.json(spark.createDataset(Seq(
      """{"k":1,"v":1}"""
    )))

    df.potatoWrite.format("redis")
      .option("hosts", "10.111.149.43:10001")
      .option("auth", "foobared")
      .save()

    df.show(10)

    TimeUnit.DAYS.sleep(1)
  }
}
