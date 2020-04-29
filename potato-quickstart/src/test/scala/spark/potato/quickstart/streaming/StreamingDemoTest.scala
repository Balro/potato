package spark.potato.quickstart.streaming

import org.junit.Test
import spark.potato.common.util.LocalLauncherUtil

class StreamingDemoTest {
  @Test
  def localTest(): Unit = {
    LocalLauncherUtil.localTest(StreamingDemo, "/streaming/StreamingDemo.properties")
  }
}
