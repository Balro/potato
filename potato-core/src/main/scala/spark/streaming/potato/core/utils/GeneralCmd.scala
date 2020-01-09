package spark.streaming.potato.core.utils

import scala.collection.mutable

trait GeneralCmd {
  val props = mutable.HashMap.empty[String, String]
  val actions = mutable.HashSet.empty[Action]
  addActions()

  def addActions(): Unit

  def main(args: Array[String]): Unit = {

  }


  case class Action(name: String, action: Map[String, String] => Unit = _ => Unit) {
    override def equals(obj: Any): Boolean = {
      obj match {
        case value: Action => value.name.toLowerCase == name.toLowerCase
        case _ => false
      }
    }
  }

}
