package spark.potato.common.spark.template

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import spark.potato.common.spark.service.ServiceManager

trait ServiceFunction {
  // 用于管理附加服务。
  implicit lazy val serviceManager: ServiceManager = new ServiceManager()

  trait WithService[T] {
    /**
     * 注册同时启动默认配置中的附加服务。
     */
    def withService: T

    /**
     * 停止附加服务后停止包装对象。
     */
    def stopWithService(): Unit
  }

  class ContextWithService(sc: SparkContext) extends WithService[SparkContext] {
    override def withService: SparkContext = {
      serviceManager.sc(sc).registerBySparkConf(sc.getConf)
      serviceManager.start()
      sc
    }

    override def stopWithService(): Unit = {
      serviceManager.stop()
      sc.stop()
    }
  }

  implicit def _withService(sc: SparkContext): WithService[SparkContext] = new ContextWithService(sc)

  class StreamingWithService(ssc: StreamingContext) extends WithService[StreamingContext] {
    override def withService: StreamingContext = {
      serviceManager.ssc(ssc).registerBySparkConf(ssc.sparkContext.getConf)
      serviceManager.start()
      ssc
    }

    override def stopWithService(): Unit = {
      serviceManager.stop()
      ssc.stop()
    }
  }

  implicit def _withService(ssc: StreamingContext): WithService[StreamingContext] = new StreamingWithService(ssc)

}
