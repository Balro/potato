package spark.streaming.potato.common.utils

import org.junit.Test

class DingRobotTest {
  @Test
  def dingTest1(): Unit = {
    DingRobot.ding("2aa713587501102395004b0f87650cc5509b0d99af25868921d6509020785483",
      "token -> 未at所有人，无at电话。")
  }

  @Test
  def dingTest2(): Unit = {
    DingRobot.ding("https://oapi.dingtalk.com/robot/send?access_token=2aa713587501102395004b0f87650cc5509b0d99af25868921d6509020785483",
      "url -> 未at所有人，无at电话。")
  }

  @Test
  def dingTest3(): Unit = {
    DingRobot.ding("2aa713587501102395004b0f87650cc5509b0d99af25868921d6509020785483",
      "token -> at所有人，无at电话。", atAll = true)
  }

  @Test
  def dingTest4(): Unit = {
    DingRobot.ding("2aa713587501102395004b0f87650cc5509b0d99af25868921d6509020785483",
      "token -> 未at所有人，有at电话。", phones = Array("13051419527"))
  }

  @Test
  def dingTest5(): Unit = {
    DingRobot.ding("2aa713587501102395004b0f87650cc5509b0d99af25868921d6509020785483",
      "token -> at所有人，有at电话。", atAll = true, phones = Array("13051419527"))
  }

}
