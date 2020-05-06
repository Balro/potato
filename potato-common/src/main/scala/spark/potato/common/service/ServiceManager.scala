package spark.potato.common.service

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

  private def notNull[T](any: T): T = {
    if (any == null)
      throw UnknownServiceException(s"Found null object, cannot serve.")
    any
  }

  /**
   * 启动托管服务。
   *
   * @param servicesName  需要启动托管服务的名称，如为空，则启动全部托管服务。
   * @param check         是否调用checkAndStart()方法，否则直接调用start()。
   * @param stopOnJVMExit 启动的服务是否在jvm退出时停止。
   */
  def start(servicesName: Seq[String] = Seq.empty[String], check: Boolean = true, stopOnJVMExit: Boolean = true): Unit =
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

      if (servicesName.isEmpty) {
        services.foreach(service => internalStart(service._2, check, stopOnJVMExit))
      } else {
        servicesName.foreach { name =>
          internalStart(services.getOrElse(name, throw new NoSuchElementException(s"Service not served $name")), check, stopOnJVMExit)
        }
      }
    }

  /**
   * 停止托管服务。
   *
   * @param servicesName 要停止的服务名称，若为空，则停止所有托管服务。
   * @param check        是否调用checkAndStop()方法，否则直接调用stop()。
   */
  def stop(servicesName: Seq[String] = Seq.empty[String], check: Boolean = true): Unit = this.synchronized {
    def internalStop(service: Service, check: Boolean): Unit = {
      if (check)
        service.checkAndStop()
      else
        service.stop()
      logInfo(s"Stop service $service")
    }

    if (servicesName.isEmpty) {
      services.foreach { service =>
        internalStop(service._2, check)
      }
    } else {
      servicesName.foreach { name =>
        internalStop(services.getOrElse(name, throw new NoSuchElementException(s"Service not served $name")), check)
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
  def registerByInstance(name: String, service: Service): Service = this.synchronized {
    if (services.contains(name)) throw ServiceAlreadyRegisteredException(s"Service $name already registered, please check.")
    service match {
      case serv: GeneralService => serv.serve(notNull(conf))
      case serv: ContextService => serv.serve(notNull(sc))
      case serv: StreamingService => serv.serve(ssc)
      case unknwon => throw UnknownServiceException(s"Unknown service $name:${unknwon.getClass}")
    }
    services += (name -> service)
    logInfo(s"Service $name:${service.getClass} successfully registered.")
    service
  }

  /**
   * 通过类名注册服务。
   */
  def registerByClass(name: String, clazz: Class[_]): Service = this.synchronized {
    registerByInstance(name, clazz.newInstance().asInstanceOf[Service])
  }

  /**
   * 通过预置服务名称注册附加服务。
   */
  def registerAdditionalServices(services: Set[String]): Unit = this.synchronized {
    logInfo("RegisterAdditionalServices method called.")

    import scala.collection.JavaConversions.iterableAsScalaIterable

    val loadedServices = ServiceLoader.load(classOf[Service]).map(s => s.serviceName -> s).toMap
    services.foreach { name =>
      registerByInstance(name, loadedServices.getOrElse(name, throw UnknownServiceException(s"Unknown service $name")))
    }
  }

  /**
   * 根据类全限定名注册自定义服务。
   */
  def registerCustomServices(services: Set[String]): Unit = this.synchronized {
    logInfo("RegisterCustomServices method called.")
    services.map(_.trim)
      .foreach { f =>
        registerByClass(f, Class.forName(f))
      }
  }

  /**
   * 初始化附加服务管理器，并启动。
   *
   * @param conf     提取附加服务列表的SparkConf。
   * @param startNow 是否即时启动附加服务，如配置为false，则须在注册完毕后手动启动serviceManager。
   */
  def registerServices(conf: SparkConf, startNow: Boolean = true): Unit = this.synchronized {
    if (conf.get(POTATO_COMMON_ADDITIONAL_SERVICES_KEY, "false").toLowerCase() == "false") {
      logWarning(
        s"""
           |Register additional service failed because conf key $POTATO_COMMON_ADDITIONAL_SERVICES_KEY not exist or equals false.
           |""".stripMargin)
    } else {
      registerAdditionalServices(conf.get(POTATO_COMMON_ADDITIONAL_SERVICES_KEY).split(",").map(_.trim).toSet)
    }

    if (conf.get(POTATO_COMMON_CUSTOM_SERVICES_CLASS_KEY, "false").toLowerCase() == "false") {
      logWarning(
        s"""
           |Register custom service failed because conf key $POTATO_COMMON_CUSTOM_SERVICES_CLASS_KEY not exist or equals false.
           |""".stripMargin)
    } else {
      registerCustomServices(conf.get(POTATO_COMMON_CUSTOM_SERVICES_CLASS_KEY).split(",").map(_.trim).toSet)
    }

    if (startNow) start()
  }
}
