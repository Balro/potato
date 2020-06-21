package potato.demo.streaming

import org.junit.Test
import potato.spark.util.LocalLauncherUtil

class KafkaSourceStreamingDemoTest {
  @Test
  def localTest(): Unit = {
    LocalLauncherUtil.launch(KafkaSourceStreamingDemo, propFile = "/potato/demo/streaming/KafkaSourceStreamingDemo.properties")
  }
}
