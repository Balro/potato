package potato.spark.template

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import potato.spark.streaming.StreamingUtil


/**
 * 包含基于SparkConf,SparkContext,StreamingContext,SparkSession等功能的基本类。
 */
trait BaseFunction {
  def createConf: SparkConf = new SparkConf()

  def createSC(conf: SparkConf = createConf): SparkContext = SparkContext.getOrCreate(conf)

  def createSSC(conf: SparkConf = createConf, dur: Long = -1): StreamingContext = StreamingUtil.createStreamingContext(conf, dur)

  def createSpark(conf: SparkConf = createConf): SparkSession = SparkSession.builder().config(conf).getOrCreate()

  class SSCFunction(ssc: StreamingContext) {
    def startAndWait(timeout: Long = 0): Unit = {
      ssc.start()
      if (timeout > 0)
        ssc.awaitTerminationOrTimeout(timeout)
      else
        ssc.awaitTermination
    }
  }

  implicit def toSSCFunction(ssc: StreamingContext): SSCFunction = new SSCFunction(ssc)

}
