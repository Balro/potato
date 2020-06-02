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
    def withService: T

    /**
     * 停止附加服务后停止包装对象。
     */
    def stopWithService(): T
  }

  class ContextWithService(sc: SparkContext) extends WithService[SparkContext] {
    override def withService: SparkContext = {
      serviceManager.sc(sc).registerBySparkConf(sc.getConf)
      sc
    }

    override def stopWithService(): SparkContext = {
      serviceManager.stop()
      sc.stop()
      sc
    }
  }

  implicit def _withService(sc: SparkContext): WithService[SparkContext] = new ContextWithService(sc)

  class StreamingWithService(ssc: StreamingContext) extends WithService[StreamingContext] {
    override def withService: StreamingContext = {
      serviceManager.ssc(ssc).registerBySparkConf(ssc.sparkContext.getConf)
      ssc
    }

    override def stopWithService(): StreamingContext = {
      serviceManager.stop()
      ssc.stop()
      ssc
    }
  }

  implicit def _withService(ssc: StreamingContext): WithService[StreamingContext] = new StreamingWithService(ssc)

}
