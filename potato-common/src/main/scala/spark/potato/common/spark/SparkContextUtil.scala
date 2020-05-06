package spark.potato.common.spark

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import spark.potato.common.threads.ExitCleanUtil

object SparkContextUtil extends Logging {
  def stopOnJVMExit(sc: SparkContext): Unit = {
    logInfo(s"Register stop when shutdown on sc $sc")
    ExitCleanUtil.cleanWhenShutdown("stop sc", {
      sc.stop()
    })
  }
}
