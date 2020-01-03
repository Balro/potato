package spark.streaming.potato.source.kafka.offsets

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

class OffsetsUpdateListener(manager: OffsetsManager) extends StreamingListener with Logging {
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logInfo(s"Update offsets on batch completed.")
    manager.updateOffsetsByDelay(batchCompleted.batchInfo.batchTime.milliseconds)
  }
}
