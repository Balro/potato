package spark.potato.template.batch

import org.junit.Test
import spark.potato.common.util.LocalLauncherUtil
import spark.potato.common.util.ContextUtil._

object BatchTemplateTest extends BatchTemplate {
  override def doWork(): Unit = {
    val sc = createContext().stopWhenShutdown
    val rdd = sc.makeRDD(1 until 10)
    println(rdd.sum())
  }
}

class BatchTemplateTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.test(BatchTemplateTest)
  }
}
