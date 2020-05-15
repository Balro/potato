package spark.potato.common.spark

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import spark.potato.common.threads.JVMCleanUtil

object SparkContextUtil extends Logging {
  def stopOnJVMExit(sc: SparkContext): Unit = {
    logInfo(s"Register stop when shutdown on sc $sc")
    JVMCleanUtil.cleanWhenShutdown("stop sc", {
      sc.stop()
    })
  }
}
