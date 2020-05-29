package spark.potato.template.batch

import org.junit.Test
import spark.potato.common.spark.LocalLauncherUtil
import spark.potato.common.spark.SparkContextUtil._
import spark.potato.template.PotatoTemplate

object BatchTemplateTest extends PotatoTemplate {
  override def main(args: Array[String]): Unit = {
    val sc = defaultSparkContext
    stopOnJVMExit(sc)
    val rdd = sc.makeRDD(1 until 10)
    println(rdd.sum())
  }
}

class BatchTemplateTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.localTest(BatchTemplateTest)
  }
}
