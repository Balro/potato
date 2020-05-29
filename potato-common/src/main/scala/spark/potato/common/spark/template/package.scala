package spark.potato.common.spark

import org.apache.spark.internal.Logging

package object template {

  /**
   * 基本模板，强制子类实现main方法，无其他副作用。
   */
  abstract class BaseTemplate extends Logging {
    def main(args: Array[String]): Unit
  }

  /**
   * 最大化模板，包含了所有支持的功能。
   */
  abstract class FullTemplate extends BaseTemplate with ServiceFunction with CleanerFunction

}
