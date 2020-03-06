package spark.potato.common.service

import org.apache.spark.streaming.StreamingContext

trait StreamingService extends Service {
  /**
   * 初始化服务。
   */
  def serve(ssc: StreamingContext): StreamingService
}
