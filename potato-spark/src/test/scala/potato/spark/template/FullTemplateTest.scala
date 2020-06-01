package potato.spark.template

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import potato.spark.service.ContextService
import potato.spark.conf._
import potato.spark.util.LocalLauncherUtil

object FullTemplateApp extends FullTemplate {
  override def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate(new SparkConf()).withService
    println(sc.parallelize(sc.getConf.get("spark.test.numbers").split(",").map(_.toInt)).sum())
    clean("stop app", sc.stopWithService)
  }
}

class TestService extends ContextService {
  /**
   * 初始化服务。
   */
  override def serve(sc: SparkContext): ContextService = this

  override val serviceName: String = "test service"

  /**
   * 建议实现为幂等操作，有可能多次调用start方法。
   * 或者直接调用checkAndStart()方法。
   */
  override def start(): Unit = println(s"$this started.")

  /**
   * 建议实现为幂等操作，有可能多次调用stop方法。
   * 或者直接调用checkAndStop()方法。
   */
  override def stop(): Unit = println(s"$this stopped.")
}

class FullTemplateTest {
  @Test
  def mainTest(): Unit = {
    LocalLauncherUtil.launch(FullTemplateApp,
      propFile = "src/test/resources/FullTemplateTest.properties",
      conf = Map(POTATO_SPARK_CUSTOM_SERVICES_CLASS_KEY -> "potato.spark.template.TestService"))
  }
}