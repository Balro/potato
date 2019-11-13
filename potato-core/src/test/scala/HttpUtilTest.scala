import scalaj.http.{Http, HttpRequest, HttpResponse}
import spark.streaming.potato.common.HttpUtil

object HttpUtilTest {
  def main(args: Array[String]): Unit = {
    val p: HttpResponse[String] = Http("https://www.baidu.com").postForm(Seq()).asString
    println(p.body)
  }
}
