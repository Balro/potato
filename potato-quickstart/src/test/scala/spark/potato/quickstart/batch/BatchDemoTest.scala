package spark.potato.quickstart.batch

import batch.BatchDemo
import org.junit.Test
import spark.potato.common.spark.LocalLauncherUtil

class BatchDemoTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.launch(BatchDemo, propFile = "/batch/BatchDemo.properties")
  }
}
