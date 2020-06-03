package quickstart.batch

import org.junit.Test
import potato.spark.util.LocalLauncherUtil

class BatchDemoTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.launch(BatchDemo, propFile = "/quickstart/batch/BatchDemo.properties")
  }
}
