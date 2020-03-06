package spark.potato.common.service

import org.apache.spark.SparkContext

trait ContextService extends Service {
  /**
   * 初始化服务。
   */
  def serve(sc: SparkContext): ContextService
}
