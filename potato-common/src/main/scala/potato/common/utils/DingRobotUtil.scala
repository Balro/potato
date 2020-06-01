package potato.common.utils

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import scalaj.http.{Http, HttpResponse}

/**
 * 钉钉机器人工具。
 */
object DingRobotUtil {
  val url = "https://oapi.dingtalk.com/robot/send"

  /**
   * @param token  机器人token。
   * @param msg    通知信息。
   * @param atAll  是否@所有人。
   * @param phones 需要@的电话，如atAll为true，则次项无效(钉钉内部逻辑，代码无处理)。
   * @return
   */
  def ding(token: String, msg: String, atAll: Boolean = false, phones: Array[String] = Array.empty): HttpResponse[String] = {
    var token_ = null.asInstanceOf[String]
    if (token.startsWith("http"))
      token_ = token.substring(token.lastIndexOf("access_token=") + 13)
    else
      token_ = token
    Http(url)
      .header("Content-Type", "application/json").param("access_token", token_)
      .postData(mkMsg(msg, atAll, phones)).asString
  }

  private def mkMsg(msg: String, atAll: Boolean, phones: Array[String]): String = {
    write(Map(
      "msgtype" -> "text",
      "text" -> Map("content" -> msg),
      "at" -> Map(
        "atMobiles" -> phones.toSeq,
        "isAtAll" -> atAll
      )
    ))(DefaultFormats)
  }
}
