package spark.potato.lock

import org.junit.Test
import spark.potato.lock.runninglock.RunningLockCmd

class RunningLockCmdTest {
  @Test
  def clearTest(): Unit = {
    println(System.getenv("potato_conf_file"))
    RunningLockCmd.main(Array("clear"))
  }
}
