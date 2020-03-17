package spark.potato.common.service

import java.util.NoSuchElementException

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.potato.common.exception.UnknownServiceException
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
  def conf(conf: Map[String, String]): ServiceManager = {
    this.conf = conf
    this
  }

  /**
   * 初始化可用配置参数，用于创建service实例。
   */
  def conf(conf: SparkConf): ServiceManager = {
    this.conf = conf.getAll.toMap
    this
  }

  /**
   * 初始化可用配置参数，用于创建service实例。
   */
  def sc(sc: SparkContext, overrideConf: Boolean = true): ServiceManager = {
    this.sc = sc
    if (overrideConf)
      conf = sc.getConf.getAll.toMap
    this
  }

  /**
   * 初始化可用配置参数，用于创建service实例。
   */
  def ssc(ssc: StreamingContext, overrideConf: Boolean = true): ServiceManager = {
    this.ssc = ssc
    sc = ssc.sparkContext
    if (overrideConf)
      conf = sc.getConf.getAll.toMap
    this
  }

  /**
   * 通过类型创建service实例。
   */
  def serve(clazz: Class[_]): Service = {
    val service = if (classOf[GeneralService].isAssignableFrom(clazz)) {
      clazz.newInstance().asInstanceOf[GeneralService].serve(checkNullable(conf))
    } else if (classOf[ContextService].isAssignableFrom(clazz)) {
      clazz.newInstance().asInstanceOf[ContextService].serve(checkNullable(sc))
    } else if (classOf[StreamingService].isAssignableFrom(clazz)) {
      clazz.newInstance().asInstanceOf[StreamingService].serve(checkNullable(ssc))
    } else {
      throw UnknownServiceException(s"Unknown service $clazz")
    }
    services += (clazz.getName -> service)
    logInfo(s"Successful serve service $service")
    service
  }

  /**
   * 通过类型名称创建service实例。
   */
  def serve(clazzName: String): Service = {
    serve(Class.forName(clazzName))
  }

  private def checkNullable[T](any: T): T = {
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
   * 初始化附加服务管理器，并启动。
   *
   * @param conf     提取附加服务列表的SparkConf。
   * @param startNow 是否即时启动附加服务，如配置为false，则须在注册完毕后手动启动serviceManager。
   */
  def registerAdditionalServices(conf: SparkConf, startNow: Boolean = true): Unit = {
    if (!conf.contains(POTATO_COMMON_ADDITIONAL_SERVICES_KEY)) {
      logWarning(s"Register additional service failed because conf key $POTATO_COMMON_ADDITIONAL_SERVICES_KEY not found.")
      return
    } else if (conf.get(POTATO_COMMON_ADDITIONAL_SERVICES_KEY).toUpperCase() == "FALSE") {
      logWarning(s"Register additional service failed because conf key $POTATO_COMMON_ADDITIONAL_SERVICES_KEY is false.")
      return
    }
    logInfo("InitAdditionalServices method called.")
    conf.get(POTATO_COMMON_ADDITIONAL_SERVICES_KEY)
      .split(",")
      .map(_.trim)
      .foreach { f =>
        serve(f)
      }
    if (startNow) start()
  }
}
