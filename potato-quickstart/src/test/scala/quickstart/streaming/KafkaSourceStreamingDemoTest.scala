package quickstart.streaming

import org.junit.Test
import potato.spark.util.LocalLauncherUtil

class KafkaSourceStreamingDemoTest {
  @Test
  def localTest(): Unit = {
    LocalLauncherUtil.launch(KafkaSourceStreamingDemo, propFile = "/quickstart/streaming/KafkaSourceStreamingDemo.properties")
  }
}
