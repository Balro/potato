package spark.potato.template.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.junit.Test
import spark.potato.common.spark.LocalLauncherUtil
import spark.potato.common.spark.streaming.StreamingUtil
import spark.potato.template.PotatoTemplate

import scala.collection.mutable

object StreamingTemplateTest extends PotatoTemplate with StreamingFunction {
  override def main(args: Array[String]): Unit = {
    val ssc = createStreamingContext(defaultSparkConf, durationMs = 5000)

    val source = ssc.queueStream(queue)
    source.print()

    ssc.start()
    while (!ssc.sparkContext.isStopped) {
      queue += ssc.sparkContext.makeRDD(Seq(new Date().toString))
      TimeUnit.MILLISECONDS.sleep(StreamingUtil.getBatchDuration(ssc).milliseconds)
    }
    ssc.awaitTermination()
  }

  private val queue = mutable.Queue.empty[RDD[String]]
}

class StreamingTemplateTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.localTest(StreamingTemplateTest)
  }
}
