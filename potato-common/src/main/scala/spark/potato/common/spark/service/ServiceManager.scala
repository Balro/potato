package spark.potato.common.spark.service

import java.util.{NoSuchElementException, ServiceLoader}

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.potato.common.exception.{ServiceAlreadyRegisteredException, UnknownServiceException}
import spark.potato.common.conf._

import scala.collection.mutable

/**
 * 通过传入类型或类型名称，获取指定服务的实例。
 */
class ServiceManager extends Logging {
  private var conf: Map[String, String] = _
  private var sc: SparkContext = _
  private var ssc: StreamingContext = _
  private val services: mutable.HashMap[String, Service] = mutable.HashMap.empty[String, Service]

  /**
   * 初始化可用配置参数，用于创建service实例。
   */
  def conf(conf: Map[String, String]): ServiceManager = this.synchronized {
    this.conf = conf
    this
  }

  /**
   * 初始化可用配置参数，用于创建service实例。
   */
  def conf(conf: SparkConf): ServiceManager = this.synchronized {
    this.conf = conf.getAll.toMap
    this
  }

  /**
   * 初始化可用配置参数，用于创建service实例。
   */
  def sc(sc: SparkContext, overrideConf: Boolean = true): ServiceManager = this.synchronized {
    this.sc = sc
    if (overrideConf)
      conf = sc.getConf.getAll.toMap
    this
  }

  /**
   * 初始化可用配置参数，用于创建service实例。
   */
  def ssc(ssc: StreamingContext, overrideConf: Boolean = true): ServiceManager = this.synchronized {
    this.ssc = ssc
    sc = ssc.sparkContext
    if (overrideConf)
      conf = sc.getConf.getAll.toMap
    this
  }

  private def notNull[T](t: T): T = {
    if (t == null)
      throw UnknownServiceException(s"Found null object, cannot serve.")
    t
  }

  /**
   * 启动托管服务。
   *
   * @param ids           需要启动托管服务的名称，如为空，则启动全部托管服务。
   * @param check         是否调用checkAndStart()方法，否则直接调用start()。
   * @param stopOnJVMExit 启动的服务是否在jvm退出时停止。
   */
  def start(ids: Seq[String] = Seq.empty[String], check: Boolean = true, stopOnJVMExit: Boolean = true): Unit =
    this.synchronized {
      def internalStart(service: Service, check: Boolean, stopOnJVMExit: Boolean): Unit = {
        if (stopOnJVMExit) {
          service.startAndStopOnJVMExit(check)
          logInfo(s"Start service with jvmexit $service")
        } else {
          if (check)
            service.checkAndStart()
          else
            service.start()
          logInfo(s"Start service $service")
        }
      }

      if (ids.isEmpty) {
        services.foreach(service => internalStart(service._2, check, stopOnJVMExit))
      } else {
        ids.foreach { id =>
          internalStart(services.getOrElse(id, throw new NoSuchElementException(s"Service not served $id")), check, stopOnJVMExit)
        }
      }
    }

  /**
   * 停止托管服务。
   *
   * @param ids   要停止的服务名称，若为空，则停止所有托管服务。
   * @param check 是否调用checkAndStop()方法，否则直接调用stop()。
   */
  def stop(ids: Seq[String] = Seq.empty[String], check: Boolean = true): Unit = this.synchronized {
    def internalStop(service: Service, check: Boolean): Unit = {
      if (check)
        service.checkAndStop()
      else
        service.stop()
      logInfo(s"Stop service $service")
    }

    if (ids.isEmpty) {
      services.foreach { service =>
        internalStop(service._2, check)
      }
    } else {
      ids.foreach { id =>
        internalStop(services.getOrElse(id, throw new NoSuchElementException(s"Service not served $id")), check)
      }
    }
  }

  /**
   * 停止并清理所有托管服务。
   */
  def clear(): Unit = this.synchronized {
    stop()
    services.clear()
  }

  /**
   * 通过服务实例注册服务。
   */
  def registerByInstance(id: String, service: Service): Service = this.synchronized {
    if (services.contains(id)) throw ServiceAlreadyRegisteredException(s"Service $id already registered, please check.")
    service match {
      case serv: GeneralService => serv.serve(notNull(conf))
      case serv: ContextService => serv.serve(notNull(sc))
      case serv: StreamingService => serv.serve(ssc)
      case unknwon => throw UnknownServiceException(s"Unknown service $id:${unknwon.getClass}")
    }
    services += (id -> service)
    logInfo(s"Service $id:${service.getClass} successfully registered.")
    service
  }

  /**
   * 通过类名注册服务。
   */
  def registerByClass(id: String, clazz: Class[_]): Service = this.synchronized {
    registerByInstance(id, clazz.newInstance().asInstanceOf[Service])
  }

  /**
   * 通过预置服务名称注册附加服务。
   */
  def registerByName(id: String, name: String): Unit = this.synchronized {
    import scala.collection.JavaConversions.iterableAsScalaIterable
    val loadedServices = ServiceLoader.load(classOf[Service]).map(s => s.serviceName -> s).toMap
    registerByInstance(id, loadedServices.getOrElse(name, throw UnknownServiceException(s"Unknown service $name")))
  }

  /**
   * 根据类全限定名注册自定义服务。
   */
  def registerByClassName(id: String, className: String): Unit = this.synchronized {
    registerByClass(id, Class.forName(className))
  }

  /**
   * 初始化附加服务管理器，并启动。
   *
   * @param conf 由参数[[POTATO_COMMON_ADDITIONAL_SERVICES_KEY]]和[[POTATO_COMMON_CUSTOM_SERVICES_CLASS_KEY]]读取服务列表并注册。
   */
  def registerBySparkConf(conf: SparkConf): Unit = this.synchronized {
    if (conf.get(POTATO_COMMON_ADDITIONAL_SERVICES_KEY, "false").toLowerCase() != "false")
      conf.get(POTATO_COMMON_ADDITIONAL_SERVICES_KEY).split(",").map(_.trim).foreach(f => registerByName(f, f))

    if (conf.get(POTATO_COMMON_CUSTOM_SERVICES_CLASS_KEY, "false").toLowerCase() != "false")
      conf.get(POTATO_COMMON_CUSTOM_SERVICES_CLASS_KEY).split(",").map(_.trim).foreach(f => registerByClassName(f, f))
  }
}
