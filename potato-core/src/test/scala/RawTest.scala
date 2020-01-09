import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RawTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test").setMaster("local[10]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val stream = KafkaUtils.createDirectStream(ssc, Map(
      "bootstrap.servers" -> "test02:9092"
    ), Set("test"))

    stream.transform(f => f).foreachRDD { rdd =>
      println("====" + 1 / (rdd.count() - 1))
    }

    ssc.start()
    val runner = new Runnable {
      override def run(): Unit = {
        while (true) {
          println("======")
          TimeUnit.SECONDS.sleep(1)
        }
      }
    }

    val t = new Thread(runner)
    t.start()
    println("========== stop ========")
    try {
      ssc.awaitTermination()
    } finally {
      t.interrupt()
    }
    println("========== stop ========")
    //    ssc.stop()
    println("========== stop ========")

  }
}
