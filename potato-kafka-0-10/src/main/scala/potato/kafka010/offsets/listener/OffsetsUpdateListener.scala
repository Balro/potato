package potato.kafka010.offsets.listener

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import potato.kafka010.offsets.manager.OffsetsManager

/**
 * 集成OffsetsManager，用于批次正常结束后提交offsets。
 * BUG!
 * 如果批次报错后不退出，而继续执行后续批次，则后续批次若执行正常，则将正常提交offsets，使异常批次的offsets丢失。
 * 为此，一定检查程序编码，批次异常后是否退出作业，若可接受异常批次offsets丢失，可忽略次bug。
 *
 * @param manager 用于提交offsets的OffsetsManager。
 */
class OffsetsUpdateListener(manager: OffsetsManager) extends StreamingListener with Logging {
  // 用于标记失败作业的出现，避免出现误提交offset的bug。
  private val canUpdate = new AtomicBoolean(true)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = this.synchronized {
    logDebug(batchCompleted.batchInfo.toString)
    val errs = batchCompleted.batchInfo.outputOperationInfos.filter {
      _._2.failureReason.isDefined
    }
    if (errs.isEmpty) {
      if (canUpdate.get()) {
        manager.updateOffsetsByTime(batchCompleted.batchInfo.batchTime.milliseconds)
        logInfo(s"Update offsets on batch completed")
      } else {
        logError(s"Cannot update offsets for batch ${batchCompleted.batchInfo.batchTime}because canUpdate is false")
      }
    } else {
      canUpdate.compareAndSet(true, false)
      logWarning(s"Update offsets on batch ${batchCompleted.batchInfo.batchTime.milliseconds} failed " +
        s"becase of $errs. Set canUpdate to false")
    }
  }
}
