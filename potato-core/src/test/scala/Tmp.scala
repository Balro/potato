import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Tmp0 {
  def main(args: Array[String]): Unit = {
    val a = new mutable.LinkedHashMap[Long, String]()
    a += 1L -> "a"
    println(a)
    println(a.head)
    println(a.last)
    a -= a.head._1
    println(a)
  }
}
