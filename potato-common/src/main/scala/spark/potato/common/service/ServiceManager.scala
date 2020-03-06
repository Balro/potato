package spark.potato.common.service

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.potato.common.exception.UnknownServiceException

/**
 * 通过传入类型或类型名称，获取指定服务的实例。
 */
class ServiceManager {
  private var conf: Map[String, String] = _
  private var sc: SparkContext = _
  private var ssc: StreamingContext = _

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
    conf = sc.getConf.getAll.toMap
    this.sc = sc
    if (overrideConf) this.conf = sc.getConf.getAll.toMap
    this
  }

  /**
   * 初始化可用配置参数，用于创建service实例。
   */
  def ssc(ssc: StreamingContext, overrideConf: Boolean = true): ServiceManager = {
    conf = sc.getConf.getAll.toMap
    sc = ssc.sparkContext
    this.ssc = ssc
    if (overrideConf) this.conf = sc.getConf.getAll.toMap
    this
  }

  /**
   * 通过类型创建service实例。
   */
  def serve(clazz: Class[_]): Service = {
    (if (classOf[GeneralService].isAssignableFrom(clazz)) {
      clazz.newInstance().asInstanceOf[GeneralService].serve(checkNullable(conf))
    } else if (classOf[ContextService].isAssignableFrom(clazz)) {
      clazz.newInstance().asInstanceOf[ContextService].serve(checkNullable(sc))
    } else if (classOf[StreamingService].isAssignableFrom(clazz)) {
      clazz.newInstance().asInstanceOf[StreamingService].serve(checkNullable(ssc))
    } else {
      throw UnknownServiceException(s"Unknown service $clazz")
    })
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
}
