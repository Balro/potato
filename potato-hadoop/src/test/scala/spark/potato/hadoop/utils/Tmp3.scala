package spark.potato.hadoop.utils

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.sql.SparkSession
import spark.potato.common.threads.DaemonThreadFactory

import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

object Tmp3 {
  implicit val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(
    Executors.newFixedThreadPool(3, DaemonThreadFactory)
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._
    val ds = spark.range(start = 0, end = 100, step = 1, numPartitions = 10)
    //    0 until 10 foreach { _ =>
    //      ds.map { f =>
    //        TimeUnit.SECONDS.sleep(f % 2)
    //        f
    //      }.count()
    //    }
    //    0 until 10 foreach { _ =>
    //      new Thread(new Runnable() {
    //        override def run(): Unit = {
    //          ds.map { f =>
    //            TimeUnit.SECONDS.sleep(f % 2)
    //            f
    //          }.count()
    //        }
    //      }).start()
    //      TimeUnit.MILLISECONDS.sleep(10)
    //    }
    var first = true
    val fs: Future[immutable.IndexedSeq[Any]] = Future.sequence(0.until(10).map { f0 =>
      val f = Future {
        try {
          ds.map { f => TimeUnit.SECONDS.sleep(f % 2); if (f0 == 2) throw new Exception("test exception"); f }.count()
        } catch {
          case e: Exception =>
            spark.close()
            throw e
        }
      }
      if (first) {
        TimeUnit.MILLISECONDS.sleep(2000)
        first = false
      }
      //      TimeUnit.MILLISECONDS.sleep(2000)
      f
    })
    Await.ready(fs, Duration.Inf)
    //    fs.value.get
    fs.value.get match {
      case Success(v) => println(v)
      case Failure(f) => f.printStackTrace()
    }
    println("====ok")

  }
}
