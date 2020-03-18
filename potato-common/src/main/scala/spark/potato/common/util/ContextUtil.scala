package spark.potato.common.util

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext

/**
 * 为SparkContext和StreamingContext提供自动stop能力。
 */
object ContextUtil extends Logging {

  class SWSDSparkContext(sc: SparkContext) {
    def stopWhenShutdown: SparkContext = {
      logInfo(s"Register stop when shutdown on sc $sc")
      CleanUtil.cleanWhenShutdown("stop sc", {
        sc.stop()
      })
      sc
    }
  }

  class SWSDStreamingContext(ssc: StreamingContext) {
    def stopWhenShutdown: StreamingContext = {
      logInfo(s"Register stop when shutdown on ssc $ssc")
      CleanUtil.cleanWhenShutdown("stop ssc", {
        ssc.stop()
      })
      ssc
    }
  }

  implicit def toSWSD(sc: SparkContext): SWSDSparkContext = {
    new SWSDSparkContext(sc)
  }

  implicit def toSWSD(ssc: StreamingContext): SWSDStreamingContext = {
    new SWSDStreamingContext(ssc)
  }

}
