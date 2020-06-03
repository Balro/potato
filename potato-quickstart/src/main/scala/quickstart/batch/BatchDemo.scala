package quickstart.batch

import potato.spark.template._

object BatchDemo extends FullTemplate {
  override def main(args: Array[String]): Unit = {
    val sc = createSC().withService.stopWhenShutdown
    println(sc.makeRDD(0 until 10).sum())
  }
}
