package spark.streaming.potato.common.utils

import org.apache.spark.internal.Logging
import org.json.JSONObject
import scalaj.http.Http

import scala.collection.JavaConversions.mapAsJavaMap

object DingRobot extends Logging {
  val url = "https://oapi.dingtalk.com/robot/send"

  def ding(token: String, msg: String, atAll: Boolean = false, phones: Array[String] = Array.empty): Boolean = {
    var token_ = null.asInstanceOf[String]
    if (token.startsWith("http"))
      token_ = token.substring(token.lastIndexOf("access_token=") + 13)
    else
      token_ = token
    val resp = Http(url)
      .header("Content-Type", "application/json").param("access_token", token_)
      .postData(mkMsg(msg, atAll, phones)).asString
    logInfo(
      s"""token -> $token, msg -> $msg, atAll -> $atAll, phones -> ${phones.mkString("[", ",", "]")}""" +
        s"""response -> "${resp.body}"""")
    resp.isSuccess
  }

  private def mkMsg(msg: String, atAll: Boolean, phones: Array[String]): String = {
    new JSONObject(mapAsJavaMap(Map(
      "msgtype" -> "text",
      "text" -> mapAsJavaMap(Map(
        "content" -> msg))
      ,
      "at" -> mapAsJavaMap(Map(
        "atMobiles" -> phones,
        "isAtAll" -> atAll
      ))
    ))).toString
  }
}
