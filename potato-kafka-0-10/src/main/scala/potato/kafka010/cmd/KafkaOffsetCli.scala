package potato.kafka010.cmd

import org.apache.commons.cli.{CommandLine, Options, ParseException}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import potato.common.cmd.CommonCliBase
import potato.kafka010.conf._
import potato.kafka010.offsets.manager.OffsetsManager
import potato.kafka010.offsets.utils.KafkaConsumerOffsetsUtil
import potato.spark.conf.SparkConfUtil.conf2Loadable

object KafkaOffsetCli extends CommonCliBase {
  override val helpWidth: Int = 120
  override val cliName: String = "KafkaOffsetCli"
  override val usageFooter: String =
    """
      |Usage:
      |  KafkaOffsetCli --prop-file prop-file --show
      |  KafkaOffsetCli --prop-file prop-file --reset --to-earliest --execute
      |  KafkaOffsetCli --prop-file prop-file --bootstrap-servers test02 --group test --show
      |  KafkaOffsetCli --bootstrap-servers test02 --group test --topics test1,test2 --storage-type kafka --show
      |Note:
      |  All config can specified in prop file.
      |  Config priority order: --conf flag > other flag > --prop-file flag.
      |""".stripMargin

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  override def initOptions(opts: Options): Unit = {
    optBuilder().longOpt("bootstrap-servers")
      .desc("Specify kafka servers to get offsets.").required.hasArg
      .add()
    optBuilder().longOpt("group")
      .desc("Consumer group to manage.").required.hasArg
      .add()
    optBuilder().longOpt("topics")
      .desc("Topics to manage, e.g. --topics tpc1,tpc2 --topics tpc3 .").required.hasArg
      .add()
    optBuilder().longOpt("storage-type")
      .desc("Supported kafka,hbase,zookeeper(not recommended).").required.hasArg
      .add()
    optBuilder().longOpt("hbase-quorum")
      .desc("HBase zookeeper quorum, e.g. zoo1,zoo2. Default: localhost").hasArg
      .add()
    optBuilder().longOpt("hbase-port")
      .desc("HBase zookeeper port, default 2181 .").hasArg
      .add()
    optBuilder().longOpt("execute")
      .desc("Used to confirm some actions, like --reset action .")
      .add()
    groupBuilder()
      .addOption(optBuilder().longOpt("show").desc("Show subscribe partitions and offset lag.").build())
      .addOption(optBuilder().longOpt("reset").desc(
        """
          |Reset offsets to earliest(--to-earliest) or latest(--to-latest).
          |It will only show the offset by default. Use --execute flag to confirm.
          |""".stripMargin.trim).build())
      .required().
      add()
    groupBuilder().addOption(optBuilder().longOpt("to-earliest").build())
      .addOption(optBuilder().longOpt("to-latest").build())
      .add()
  }

  /**
   * 根据已解析命令行参数进行处理。
   */
  override def handleCmd(cmd: CommandLine): Unit = {
    val conf = new SparkConf()

    cmd.getOptionValue("bootstrap-servers") match {
      case value: String => conf.set(POTATO_KAFKA_COMMON_BOOTSTRAP_SERVERS_KEY, value)
      case null =>
    }
    cmd.getOptionValue("group") match {
      case value: String => conf.set(POTATO_KAFKA_CONSUMER_GROUP_ID_KEY, value)
      case null =>
    }
    cmd.getOptionValues("topics") match {
      case values: Array[String] => conf.set(POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY, values.mkString(","))
      case null =>
    }
    cmd.getOptionValue("storage-type") match {
      case value: String => conf.set(POTATO_KAFKA_OFFSETS_STORAGE_TYPE_KEY, value)
      case null =>
    }
    cmd.getOptionValue("hbase-quorum") match {
      case value: String => conf.set(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_ZOO_QUORUM_KEY, value)
      case null =>
    }
    cmd.getOptionValue("hbase-port") match {
      case value: String => conf.set(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_ZOO_PORT_KEY, value)
      case null =>
    }

    val manager = new OffsetsManager(conf)

    if (cmd.hasOption("show")) {
      val a: Map[TopicPartition, Long] = KafkaConsumerOffsetsUtil.getLatestOffsets(manager.kafkaConf.toConsumerProperties, manager.subscriptions)
      val b: Map[TopicPartition, Long] = manager.committedOffsets()
      a.map(f => f._1 -> (f._2, b(f._1), f._2 - b(f._1))).groupBy(_._1.topic()).foreach { kf =>
        console(s"topic:${kf._1}")
        kf._2.toSeq.sortBy(_._1.partition()).foreach(f => console(f"\t${f._1}%-10s -> latest:${f._2._1}%-8s, committed:${f._2._2}, lag:${f._2._3}"))
        console(s"\t--------")
        console(s"\ttotal_lag:${kf._2.foldLeft(0L)((r, e) => r + e._2._3)}")
      }
    } else if (cmd.hasOption("reset")) {
      val newOffsets = {
        if (cmd.hasOption("to-earliest"))
          KafkaConsumerOffsetsUtil.getEarliestOffsets(manager.kafkaConf.toConsumerProperties, manager.subscriptions)
        else if (cmd.hasOption("to-latest"))
          KafkaConsumerOffsetsUtil.getLatestOffsets(manager.kafkaConf.toConsumerProperties, manager.subscriptions)
        else
          throw new ParseException(s"Missing argument [--to-earliest|--to-latest].")
      }
      val curOffsets = manager.committedOffsets()
      newOffsets.groupBy(_._1.topic()).foreach { kf =>
        console(s"topic:${kf._1}")
        kf._2.toSeq.sortBy(_._1.partition()).foreach(f => console(f"\t${f._1}%-10s -> old:${curOffsets.getOrElse(f._1, KafkaConsumerOffsetsUtil.invalidOffset)}%-8s, new:${f._2}"))
      }
      if (cmd.hasOption("execute")) {
        manager.updateOffsets(newOffsets)
        console("\nNew offsets updated.")
      } else
        console("\nRerun this command and use --execute flag to confirm.")
    }
  }
}
