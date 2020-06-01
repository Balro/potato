package potato.spark.util

import java.io.FileInputStream
import java.util.Properties

/**
 * 本地测试工具类。
 */
object LocalLauncherUtil {
  /**
   * @param clazz       待测试的object，必须具有main方法，不支持直接传入class。
   * @param cmdArgs     测试main方法的命令行参数。
   * @param propFile    配置文件类路径或绝对路径，配置文件中的spark.master会替换为local。
   * @param conf        配置参数，与propFile参数一起加载入SparkConf。优先级优于propFile。
   * @param masterCores local模式的核数，默认为 * 即本地cpu核数。
   * @param appName     local模式作业名  ，默认 localTest
   */
  def launch(clazz: AnyRef, cmdArgs: Array[String] = Array.empty[String],
             propFile: String = null, conf: Map[String, String] = Map.empty,
             masterCores: String = "*", appName: String = "localTest"): Unit = {
    if (propFile != null) {
      val props = new Properties()
      var propSource = clazz.getClass.getResourceAsStream(propFile)
      if (propSource == null) propSource = new FileInputStream(propFile)
      props.load(propSource)
      scala.collection.JavaConversions.propertiesAsScalaMap(props).foreach { prop =>
        System.setProperty(prop._1, prop._2)
      }
    }
    conf.foreach(f => System.setProperty(f._1, f._2))
    System.setProperty("spark.master", s"local[$masterCores]")
    System.setProperty("spark.app.name", appName)
    clazz.getClass.getMethod("main", classOf[Array[String]]).invoke(clazz, cmdArgs)
  }
}
