package spark.potato.quickstart.streaming

import org.junit.Test
import spark.potato.common.spark.LocalLauncherUtil


class KafkaSourceStreamingDemoTest {
  @Test
  def localTest(): Unit = {
    LocalLauncherUtil.launch(KafkaSourceStreamingDemo,propFile= "/streaming/KafkaSourceStreamingDemo.properties")
  }
}
