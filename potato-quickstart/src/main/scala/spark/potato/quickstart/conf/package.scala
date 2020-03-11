package spark.potato.quickstart

import spark.potato.common.conf.POTATO_PREFIX

package object conf {
  val POTATO_SUBMIT_PREFIX: String = POTATO_PREFIX + "submit."
  val POTATO_SUBMIT_BIN_KEY: String = POTATO_SUBMIT_PREFIX + "bin"
  val POTATO_SUBMIT_MAIN_CLASS_KEY: String = POTATO_SUBMIT_PREFIX + "main.class"
  val POTATO_SUBMIT_MAIN_JAR_KEY: String = POTATO_SUBMIT_PREFIX + "main.jar"
}