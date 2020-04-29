package spark.potato.common.util

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

object SparkContextUtil extends Logging {
  def stopOnJVMExit(sc: SparkContext): Unit = {
    logInfo(s"Register stop when shutdown on sc $sc")
    CleanExitUtil.cleanWhenShutdown("stop sc", {
      sc.stop()
    })
  }
}
