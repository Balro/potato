package spark.streaming.potato.core.utils

import scalaj.http.{Http, HttpResponse}

object HttpUtilTest {
  def main(args: Array[String]): Unit = {
    val p: HttpResponse[String] = Http("https://www.baidu.com").postForm(Seq()).asString
    println(p.body)
  }
}
