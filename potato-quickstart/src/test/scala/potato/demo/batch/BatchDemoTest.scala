package potato.demo.batch

import org.junit.Test
import potato.spark.util.LocalLauncherUtil

class BatchDemoTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.launch(BatchDemo, propFile = "/potato/demo/batch/BatchDemo.properties")
  }
}
