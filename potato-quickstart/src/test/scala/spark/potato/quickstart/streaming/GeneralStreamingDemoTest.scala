package spark.potato.quickstart.streaming

import org.junit.Test
import spark.potato.common.util.LocalLauncherUtil


class GeneralStreamingDemoTest {
  @Test
  def localTest(): Unit = {
    LocalLauncherUtil.test(GeneralStreamingDemo, "/streaming/GeneralStreamingDemo.properties")
  }
}
