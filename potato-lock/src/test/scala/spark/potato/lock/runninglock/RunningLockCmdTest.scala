package spark.potato.lock.runninglock

import org.junit.Test

class RunningLockCmdTest {
  @Test
  def clearTest(): Unit = {
    println(System.getenv("potato_conf_file"))
    RunningLockCmd.main(Array("clear"))
  }
}
