package spark.streaming.potato.plugins.kafka.source.offsets

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

class OffsetsUpdateListener(manager: OffsetsManager) extends StreamingListener with Logging {
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logDebug(batchCompleted.batchInfo.toString)
    val errs = batchCompleted.batchInfo.outputOperationInfos.filter {
      _._2.failureReason.isDefined
    }
    if (errs.isEmpty) {
      manager.updateOffsetsByDelay(batchCompleted.batchInfo.batchTime.milliseconds)
      logInfo(s"Update offsets on batch completed.")
    } else {
      logWarning(s"Update offsets on batch ${batchCompleted.batchInfo.batchTime.milliseconds} failed " +
        s"becase of $errs")
    }
  }
}
