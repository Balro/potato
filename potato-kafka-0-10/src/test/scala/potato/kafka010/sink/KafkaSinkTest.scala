package potato.kafka010.sink

import java.util.Date

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

import potato.common.conf.PropertiesImplicits._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class KafkaSinkTest {
  @Test
  def sinkTest(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaSinkTest")
    val ssc = new StreamingContext(conf, Seconds(1))

    val queue = mutable.Queue.empty[RDD[String]]

    val stream = ssc.queueStream(queue)
    stream.map { f =>
      println("------" + GlobalProducerCache.size())
      new ProducerRecord[String, String]("test1", f)
    }.saveToKafka(Map(
      "bootstrap.servers" -> "test02:9092",
      "key.serializer" -> classOf[StringSerializer].getName,
      "value.serializer" -> classOf[StringSerializer].getName
    ))

    ssc.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        val buff = ListBuffer.empty[String]
        for (_ <- 0 until 10) {
          buff += (new Date() + " " + System.currentTimeMillis())
        }
        val rdd = ssc.sparkContext.makeRDD(buff)
        queue += rdd
        println(s"make rdd - ${queue.size} - ${buff.size}")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
