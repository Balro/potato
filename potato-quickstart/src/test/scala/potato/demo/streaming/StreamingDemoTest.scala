package potato.demo.streaming

import org.junit.Test
import potato.spark.util.LocalLauncherUtil

class StreamingDemoTest {
  @Test
  def localTest(): Unit = {
    LocalLauncherUtil.launch(StreamingDemo, propFile = "/potato/demo/streaming/StreamingDemo.properties")
  }
}
