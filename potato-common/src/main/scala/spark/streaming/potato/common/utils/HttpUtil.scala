package spark.streaming.potato.common.utils

import scalaj.http.{Http, HttpResponse}

object HttpUtil {
  def get(url: String): HttpResponse[String] = {
    Http(url).asString
  }

  def getWithParam(url: String)(params: Map[String, String]): HttpResponse[String] = {
    Http(url).params(params).asString
  }

  def post(url: String): HttpResponse[String] = {
    Http(url).postForm(Seq()).asString
  }

  def postWithParam(url: String)(params: Seq[(String, String)]): HttpResponse[String] = {
    Http(url).postForm(params).asString
  }
}
