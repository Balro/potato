package spark.potato.quickstart.batch

import org.junit.Test
import spark.potato.common.util.LocalLauncherUtil

class BatchDemoTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.localTest(BatchDemo, "/batch/BatchDemo.properties")
  }
}
