package potato.monitor

import potato.monitor.alert.{Alert, DingAlert}
import potato.monitor.check.{Checker, ZooChecker}
import potato.monitor.conf.PotatoMonitorConf
import potato.monitor.heartbeat.{HeartBeater, RestHeartBeater}

object PotatoMonitor {
  private var conf: PotatoMonitorConf = _
  private var heartBeater: HeartBeater = _
  private var checker: Checker = _
  private var alert: Alert = _

  def main(args: Array[String]): Unit = {
    if (args.length == 0 || args(0).equals("start")) {
      conf = new PotatoMonitorConf()
      initServices(conf)
      startServices()
    }

    //    args.toList match {
    //      case ""
    //    }
  }

  def initServices(conf: PotatoMonitorConf): Unit = {
    heartBeater = new RestHeartBeater(conf)
    alert = new DingAlert(conf)
    checker = new ZooChecker(conf)
  }

  def startServices(): Unit = {
    checker.start()
    heartBeater.start()
  }
}