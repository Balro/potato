package spark.potato.quickstart.streaming

import org.junit.Test
import spark.potato.common.util.LocalLauncherUtil


class KafkaSourceStreamingDemoTest {
  @Test
  def localTest(): Unit = {
    LocalLauncherUtil.test(KafkaSourceStreamingDemo, "/streaming/KafkaSourceStreamingDemo.properties")
  }
}
