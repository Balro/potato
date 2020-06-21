package potato.spark.lock.singleton

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.{SparkConf, SparkContext}
import potato.common.cmd.CommonCliBase
import potato.common.exception.PotatoException
import potato.spark.conf._
import potato.spark.conf.SparkConfUtil.conf2Loadable

import org.json4s._
import org.json4s.jackson._

object SingletonLockCli extends CommonCliBase {
  override val helpWidth: Int = 100
  override val cliName: String = "SingletonLockCli"

  override val usageFooter: String =
    """
      |SingletonLockCli --prop-file /path/to/file --state
      |SingletonLockCli --prop-file /path/to/file --id test_app --type zookeeper --clear
      |""".stripMargin

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  override def initOptions(opts: Options): Unit = {
    groupBuilder().addOption(
      optBuilder().longOpt("state").hasArg(false)
        .desc("Show lock state.").build()
    ).addOption(
      optBuilder().longOpt("clean").hasArg(false)
        .desc("Force clean old lock, can use it to stop app.").build()
    ).required().add()
    optBuilder().longOpt("id").hasArg
      .desc("App id, default is app name.").add()
    optBuilder().longOpt("type").hasArg.required
      .desc("Specify the lock type, support type is zookeeper.").add()
    optBuilder().longOpt("zoo-quorum").hasArg
      .desc("Zookeeper quorum which the zookeeper lock will use.").add()
    optBuilder().longOpt("zoo-path").hasArg
      .desc("Zookeeper path to store the lock.").add()
    optBuilder().longOpt("prop-file").hasArg
      .desc("Job properties file.").add()
    optBuilder().longOpt("conf").hasArgs
      .desc("Other configurations, e.g. --conf k1=v1 --conf k2=v2 .").add()
  }

  /**
   * 根据已解析命令行参数进行处理。
   */
  override def handleCmd(cmd: CommandLine): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    handleValue("prop-file", conf.loadPropertyFile(_))
    handleValue("id", conf.setAppName)
    handleValue("zoo-quorum", conf.set(POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY, _))
    handleValue("zoo-path", conf.set(POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY, _),
      () => conf.set(POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY, POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_DEFAULT))
    handleValues("conf", _.foreach { f =>
      val kv = f.split("=")
      conf.set(kv(0), kv(1))
    })
    val service = new SingletonLockCliService(conf)
    val lock = handleValue("type", {
      case "zookeeper" => new ZookeeperSingletonLock(service,
        conf.get(POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY),
        POTATO_LOCK_SINGLETON_ZOOKEEPER_TIMEOUT_DEFAULT.toInt,
        conf.get(POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY),
        conf.get("spark.app.name")
      )
      case other => throw new PotatoException(s"SingletonLock not support type $other .")
    })

    implicit val fmt: Formats = DefaultFormats
    if (cmd.hasOption("state")) {
      val res = lock.getMsg
      if (res._1)
        console(s"Lock message is:\n${prettyJson(parseJson(res._2))}.")
      else
        console("Lock does not exist.")
    } else if (cmd.hasOption("clean")) {
      val res = lock.getMsg
      if (res._1) {
        console(s"Lock message is:\n${prettyJson(parseJson(res._2))}.")
        if (lock.clean())
          console("Old lock has been cleaned successfully.")
        else
          console("Old lock clean failed.")
      } else {
        console("Lock does not exist.")
      }
    }
  }

  /**
   * SingletonLockService 的空实现，用于使用lock相关api。
   */
  class SingletonLockCliService(sparkConf: SparkConf) extends SingletonLockService {
    override val sc: SparkContext = null

    override val conf: SparkConf = sparkConf

    override val serviceName: String = "SingletonLockCliService"

    /**
     * 建议实现为幂等操作，有可能多次调用stop方法。
     * 或者直接调用checkAndStop()方法。
     */
    override def stop(): Unit = Unit
  }

}
