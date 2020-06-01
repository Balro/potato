package potato.common.sender

import org.apache.spark.SparkConf
import potato.common.conf._
import potato.common.utils.DingRobotUtil

class DingDingSender(conf: SparkConf) extends Sender[String, Unit] {
  val token: String = conf.get(POTATO_COMMON_SENDER_DING_TOKEN_KEY)
  val atAll: Boolean = conf.get(POTATO_COMMON_SENDER_DING_AT_KEY, POTATO_COMMON_SENDER_DING_AT_DEFAULT) == "all"
  val phones: Array[String] = if (atAll) Array.empty else conf.get(POTATO_COMMON_SENDER_DING_AT_KEY, POTATO_COMMON_SENDER_DING_AT_DEFAULT).split(",")

  override def send(msg: String): Unit = DingRobotUtil.ding(token, msg, atAll, phones)
}
