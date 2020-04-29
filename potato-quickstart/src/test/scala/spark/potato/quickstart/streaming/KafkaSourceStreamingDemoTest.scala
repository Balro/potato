package spark.potato.quickstart.streaming

import org.junit.Test
import spark.potato.common.util.LocalLauncherUtil


class KafkaSourceStreamingDemoTest {
  @Test
  def localTest(): Unit = {
    LocalLauncherUtil.localTest(KafkaSourceStreamingDemo, "/streaming/KafkaSourceStreamingDemo.properties")
  }
}
