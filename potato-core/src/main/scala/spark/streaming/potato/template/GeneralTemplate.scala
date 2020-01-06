package spark.streaming.potato.template

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import spark.streaming.potato.context.PotatoContextUtil

abstract class GeneralTemplate extends Logging {
  var oConf: Option[SparkConf] = None
  var oSsc: Option[StreamingContext] = None

  def conf: SparkConf = oConf.get

  def ssc: StreamingContext = oSsc.get

  def main(args: Array[String]): Unit = {
    initConf(args)
    initContext(args)

    doWork(args)
    ssc.start()
    afterStart(args)
    ssc.awaitTermination()
    afterStop(args)
  }

  // 业务逻辑。
  def doWork(args: Array[String]): Unit

  def initConf(args: Array[String]): Unit = {
    logInfo("Method initConf has been called.")

    oConf = Option(new SparkConf())
  }

  def initContext(args: Array[String]): Unit = {
    logInfo("Method initContext has been called.")

    oConf match {
      case Some(sparkConf) => oSsc = Option(PotatoContextUtil.createContext(sparkConf))
      case None => throw new Exception("Spark conf is not initialized.")
    }

    if (oSsc.isEmpty)
      throw new Exception("Spark streaming context is not initialized.")
  }

  def afterStart(args: Array[String]): Unit = {
    logInfo("Method afterStart has been called.")
  }

  def afterStop(args: Array[String]): Unit = {
    logInfo("Method afterStop has been called.")
  }
}
