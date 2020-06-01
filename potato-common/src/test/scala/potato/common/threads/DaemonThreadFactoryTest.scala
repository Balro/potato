package potato.common.threads

import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

//noinspection ScalaDeprecation
object DaemonThreadFactoryTest {
  def main(args: Array[String]): Unit = {
    val executor = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)
    executor.scheduleAtFixedRate(new Runner, 0, 2, TimeUnit.SECONDS)
    TimeUnit.SECONDS.sleep(10)
    println("main exit")
  }

  class Runner extends Runnable {
    override def run(): Unit = {
      println(s"${new Date()} ${Thread.currentThread().getName} ${Thread.currentThread().isDaemon}")
    }
  }

}
