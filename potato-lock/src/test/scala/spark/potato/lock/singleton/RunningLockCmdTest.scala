package spark.potato.lock.singleton

import org.junit.Test

class RunningLockCmdTest {
  @Test
  def clearTest(): Unit = {
    println(System.getenv("potato_conf_file"))
    SingletonLockCmd.main(Array("clear"))
  }
}
