package spark.streaming.potato.plugins.hbase

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.hadoop.hbase.client.{Append, Delete, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test
import spark.streaming.potato.plugins.hbase.TableUtil._
import spark.streaming.potato.plugins.hbase.HBaseImplicits._

class TableUtilTest {
  val conf = Map(
    "hbase.zookeeper.quorum" -> "test01,test02"
  )

  @Test
  def withTableTest(): Unit = {
    withTable(conf, "test") { table =>
      table.put(new Put(Bytes.toBytes("haha")).addColumn(
        Bytes.toBytes("f1"),
        Bytes.toBytes("test"),
        Bytes.toBytes("hello")
      ))

      table.delete(new Delete(Bytes.toBytes("haha")).addColumns(
        Bytes.toBytes("f1"),
        Bytes.toBytes("test")
      ))
    }
  }

  @Test
  def withMutatorTest(): Unit = {
    //    withMutator(conf, "test") { mutator =>
    //      mutator.mutate(new Put(Bytes.toBytes("haha")).addColumn(
    //        Bytes.toBytes("f1"),
    //        Bytes.toBytes("test"),
    //        Bytes.toBytes(1L)
    //      ))
    //      mutator.mutate(new Increment(Bytes.toBytes("haha")).addColumn(
    //        Bytes.toBytes("f1"),
    //        Bytes.toBytes("test"),
    //        1
    //      ))
    //    }
    withMutator(conf, "test") { mutator =>
      mutator.mutate(new Put(Bytes.toBytes("haha")).addColumn(
        Bytes.toBytes("f1"),
        Bytes.toBytes("test"),
        Bytes.toBytes("hello")
      ))

      mutator.mutate(new Append(Bytes.toBytes("haha")).add(
        Bytes.toBytes("f1"),
        Bytes.toBytes("test"),
        Bytes.toBytes(" world")
      ))
    }
    withMutator(conf, "test") { mutator =>
      mutator.mutate(new Delete(Bytes.toBytes("haha")).addColumns(
        Bytes.toBytes("f1"),
        Bytes.toBytes("test")
      ))
    }
  }

  @Test
  def withBufferedSinkTableTest(): Unit = {
    withBufferedSinkTable(conf, "test", 100) { sinkTable =>
      for (i <- 0 until 1024) {
        sinkTable.add(new Put(Bytes.toBytes(i.toString)).addColumn(
          Bytes.toBytes("f1"),
          Bytes.toBytes("bst"),
          Bytes.toBytes("gaga")
        ))
      }
    }
  }

  @Test
  def withSynchronizedBufferedSinkTableTest(): Unit = {
    withSynchronizedBufferedSinkTable(conf, "test", 100) { sinkTable =>
      class Runner(id: Int) extends Runnable {
        override def run(): Unit = {
          for (i <- 0 until 1024) {
            sinkTable.add(new Put(Bytes.toBytes(s"${Thread.currentThread().getName}-$id:$i")).addColumn(
              Bytes.toBytes("f1"),
              Bytes.toBytes("bst"),
              Bytes.toBytes("gaga")
            ))
          }
        }
      }
      val executors = Executors.newCachedThreadPool()
      val jobNum = 10
      val jobs = 0 until jobNum map { id =>
        executors.submit(new Runner(id))
      }
      while (jobs.count { f => f.isDone } != jobNum) TimeUnit.SECONDS.sleep(1)
    }
  }

}
