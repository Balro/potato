package spark.potato.quickstart.batch

import org.junit.Test
import spark.potato.common.spark.LocalLauncherUtil

class BatchDemoTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.localTest(BatchDemo, propFile = "/batch/BatchDemo.properties")
  }
}
