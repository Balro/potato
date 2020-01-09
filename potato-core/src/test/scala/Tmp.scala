import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

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

object Tmp1 {
  implicit val intTag: ClassTag[Int] = ClassTag(classOf[Int])
  implicit val stringTag: ClassTag[String] = ClassTag(classOf[String])

  def main(args: Array[String]): Unit = {
    val f: Int => String = { f: Int => f.toString }
    import ClassTag.Int
    println(a[Int, String](1)(f, intTag, stringTag))
  }

  def a[K, R](k: K)(implicit f: K => R, kt: ClassTag[K], rt: ClassTag[R]): R = {
    doWork[K, R](k, f)
  }

  def doWork[K: ClassTag, R: ClassTag](k: K, f: K => R): R = {
    f(k)
  }
}

object Tmp2 {
  def main(args: Array[String]): Unit = {
    println(
      """
        |abcc
        |
        |dbc
      """.stripMargin.trim)
    println("----")
  }
}

object Tmp3 {
  def main(args: Array[String]): Unit = {
    val bs = "abc".getBytes()
    println(new String(bs))
  }
}

object Tmp4 {
  def main(args: Array[String]): Unit = {
    val a1 = Case("a", { () => println("hello") })
    val a2 = Case("A", () => println("word"))
    println(a1 == a2)
    println(a1 equals a2)
    println(a1 eq a2)
    a1.action()
    a2.action()
  }

  case class Case(name: String, action: () => Unit = () => Unit) {
    override def equals(obj: Any): Boolean = {
      obj match {
        case value: Case => value.name.toLowerCase == name.toLowerCase
        case _ => false
      }
    }
  }

}

