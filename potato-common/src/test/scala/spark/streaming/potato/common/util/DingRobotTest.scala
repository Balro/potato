package spark.streaming.potato.common.util

import org.junit.Test

class DingRobotTest {
  val token = "xxxx"
  val phones: Array[String] = Array("123")

  @Test
  def dingTest1(): Unit = {
    DingRobotUtil.ding(token,
      "token -> 未at所有人，无at电话。")
  }

  @Test
  def dingTest2(): Unit = {
    DingRobotUtil.ding(token,
      "url -> 未at所有人，无at电话。")
  }

  @Test
  def dingTest3(): Unit = {
    DingRobotUtil.ding(token,
      "token -> at所有人，无at电话。", atAll = true)
  }

  @Test
  def dingTest4(): Unit = {
    DingRobotUtil.ding(token,
      "token -> 未at所有人，有at电话。", phones = phones)
  }

  @Test
  def dingTest5(): Unit = {
    DingRobotUtil.ding(token,
      "token -> at所有人，有at电话。", atAll = true, phones = phones)
  }

}
