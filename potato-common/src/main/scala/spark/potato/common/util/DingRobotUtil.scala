package spark.potato.common.util

import org.apache.spark.internal.Logging
import org.json.JSONObject
import scalaj.http.Http

/**
 * 钉钉机器人工具。
 */
object DingRobotUtil extends Logging {
  val url = "https://oapi.dingtalk.com/robot/send"

  /**
   * @param token  机器人token。
   * @param msg    通知信息。
   * @param atAll  是否@所有人。
   * @param phones 需要@的电话，如atAll为true，则次项无效(钉钉内部逻辑，代码无处理)。
   * @return
   */
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
    import scala.collection.JavaConversions.mapAsJavaMap
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
