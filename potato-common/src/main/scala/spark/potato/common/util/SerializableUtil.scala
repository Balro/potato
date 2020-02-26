package spark.potato.common.util

import org.apache.spark.SparkConf

object SerializableUtil {
  /**
   * 注册第三方不可序列化类。
   * 必须在创建context之前注册。
   * 会将spark序列化引擎替换为KryoSerializer。
   */
  def register(conf: SparkConf, clazzes: Array[Class[_]]): Unit = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(clazzes)
  }
}
