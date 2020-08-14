package potato.kafka010.offsets.cmd

import org.junit.Test
import potato.kafka010.cmd.KafkaOffsetCli

class KafkaOffsetCliTest {
  @Test
  def usageTest(): Unit = {
    KafkaOffsetCli.main(Array.empty)
  }

  @Test
  def showTest(): Unit = {
    val args = Array(
      "--bootstrap-servers", "test02:9092",
      "--topics", "test1,test2",
      "--group", "test",
      "--storage-type", "hbase",
      "--hbase-quorum", "test02",
      "--hbase-port", "2181",
      "--show"
    )
    KafkaOffsetCli.main(args)
  }

  @Test
  def resetTest(): Unit = {
    val args = Array(
      "--bootstrap-servers", "test02:9092",
      "--topics", "test1,test2",
      "--group", "test",
      "--storage-type", "hbase",
      "--hbase-quorum", "test02",
      "--hbase-port", "2181",
      "--reset", "--to-latest", "--execute"
    )
    KafkaOffsetCli.main(args)
  }
}
