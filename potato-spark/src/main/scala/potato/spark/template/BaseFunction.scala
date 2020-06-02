package potato.spark.template

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import potato.spark.streaming.StreamingUtil

/**
 * 包含基于SparkConf,SparkContext,StreamingContext,SparkSession等功能的基本类。
 */
trait BaseFunction {
  lazy val defaultConf: SparkConf = new SparkConf()
  lazy val defaultSC: SparkContext = SparkContext.getOrCreate(defaultConf)
  lazy val defaultSSC: StreamingContext = StreamingUtil.createStreamingContextWithDuration(defaultConf)
  lazy val defaultSpark: SparkSession = SparkSession.builder().config(defaultConf).getOrCreate()
}
