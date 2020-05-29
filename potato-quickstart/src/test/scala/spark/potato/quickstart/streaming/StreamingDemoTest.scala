package spark.potato.quickstart.streaming

import org.junit.Test
import spark.potato.common.spark.LocalLauncherUtil

class StreamingDemoTest {
  @Test
  def localTest(): Unit = {
    LocalLauncherUtil.launch(StreamingDemo, propFile = "/streaming/StreamingDemo.properties")
  }
}
