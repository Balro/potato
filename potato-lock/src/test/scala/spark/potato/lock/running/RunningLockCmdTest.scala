package spark.potato.lock.running

import org.junit.Test

class RunningLockCmdTest {
  @Test
  def clearTest(): Unit = {
    println(System.getenv("potato_conf_file"))
    RunningLockCmd.main(Array("clear"))
  }
}
