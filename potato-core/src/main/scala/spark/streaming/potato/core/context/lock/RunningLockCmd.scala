package spark.streaming.potato.core.context.lock

import java.io.{File, FileInputStream}
import java.util.Properties

import spark.streaming.potato.core.utils.ArgsParser

object RunningLockCmd {

  def main(args: Array[String]): Unit = {
  }


  val STOP = Action("stop")

  case class Action(desc: String)

}
