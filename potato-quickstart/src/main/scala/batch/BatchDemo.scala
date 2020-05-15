package batch

import spark.potato.template.batch.BatchTemplate

object BatchDemo extends BatchTemplate {
  override def doWork(): Unit = {
    val sc = createContext()
    println(sc.makeRDD(0 until 10).sum())
  }
}
