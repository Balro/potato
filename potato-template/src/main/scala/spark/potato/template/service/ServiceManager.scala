package spark.potato.template.service

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

/**
 * 用于向SparkContext或StreamingContext添加附加服务。
 */
object ServiceManager {
  def manageContext(sc: SparkContext, services: Seq[String]): Unit = {

  }

  def manageStreaming(ssc: StreamingContext, services: Seq[String]): Unit = {

  }
}
