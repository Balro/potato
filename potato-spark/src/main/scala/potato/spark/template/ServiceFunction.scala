package potato.spark.template

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import potato.spark.service.{Service, ServiceManager}

trait ServiceFunction {
  implicit lazy val serviceManager: ServiceManager = new ServiceManager()

  def registerService(id: String, service: Service): Service = serviceManager.registerByInstance(id, service)

  def unregisterService(id: String): Service = serviceManager.unregister(id)

  def getService(id: String): Service = serviceManager.getService(id)

  def stopService(): Unit = serviceManager.stop()

  trait WithService[T] {
    /**
     * 注册同时启动默认配置中的附加服务。
     */
    def withDefaultService: T

    /**
     * 停止所有附加服务。
     */
    def stopAllService: T
  }

  class ContextWithService(sc: SparkContext) extends WithService[SparkContext] {
    override def withDefaultService: SparkContext = {
      serviceManager.sc(sc).registerBySparkConf(sc.getConf)
      sc
    }

    override def stopAllService: SparkContext = {
      serviceManager.stop()
      sc
    }
  }

  implicit def _withService(sc: SparkContext): WithService[SparkContext] = new ContextWithService(sc)

  class StreamingWithService(ssc: StreamingContext) extends WithService[StreamingContext] {
    override def withDefaultService: StreamingContext = {
      serviceManager.ssc(ssc).registerBySparkConf(ssc.sparkContext.getConf)
      ssc
    }

    override def stopAllService: StreamingContext = {
      serviceManager.stop()
      ssc
    }
  }

  implicit def _withService(ssc: StreamingContext): WithService[StreamingContext] = new StreamingWithService(ssc)
}
