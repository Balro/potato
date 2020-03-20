package spark.potato.template

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import spark.potato.common.service.ServiceManager

trait StreamingContextFunc {
  protected def createConf(): SparkConf = new SparkConf()

  protected def createStreamingContext(conf: SparkConf, durMS: Long): StreamingContext

  // 注册附加服务。
  def registerAdditionalServices(ssc: StreamingContext)(implicit manager: ServiceManager): StreamingContext = {
    manager.ssc(ssc).registerServices(ssc.sparkContext.getConf)
    ssc
  }

  def start(ssc: StreamingContext): Unit

  def afterStart(ssc: StreamingContext): Unit
}
