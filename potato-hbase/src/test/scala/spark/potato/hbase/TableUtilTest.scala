package spark.potato.hbase

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.hadoop.hbase.client.{Append, Delete, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test
import spark.potato.hbase.TableUtil._

class TableUtilTest {
  val conf = Map(
    "hbase.zookeeper.quorum" -> "test01,test02"
  )

  @Test
  def withTableTest(): Unit = {
    for (i <- 0 until 4) {
      withTable(conf, "test") { table =>
        for (j <- 0 until 2000) {
          table.put(new Put(Bytes.toBytes(s"$i-$j")).addColumn(
            Bytes.toBytes("f1"),
            Bytes.toBytes("test"),
            Bytes.toBytes("hello")
          ))
          if (j % 2 == 0)
            table.delete(new Delete(Bytes.toBytes(s"$i-$j")).addColumns(
              Bytes.toBytes("f1"),
              Bytes.toBytes("test")
            ))
        }
      }
    }
  }

  @Test
  def withMutatorTest(): Unit = {
    for (i <- 0 until 4) {
      withMutator(conf, "test") { mutator =>
        for (j <- 0 until 2000) {
          mutator.mutate(new Put(Bytes.toBytes(s"$i-$j")).addColumn(
            Bytes.toBytes("f1"),
            Bytes.toBytes("test"),
            Bytes.toBytes("hello")
          ))
          if (j % 2 == 0)
            mutator.mutate(new Delete(Bytes.toBytes(s"$i-$j")).addColumns(
              Bytes.toBytes("f1"),
              Bytes.toBytes("test")
            ))
        }
      }
    }
    //    withMutator(conf, "test") { mutator =>
    //      mutator.mutate(new Put(Bytes.toBytes("haha")).addColumn(
    //        Bytes.toBytes("f1"),
    //        Bytes.toBytes("test"),
    //        Bytes.toBytes("hello")
    //      ))
    //
    //      mutator.mutate(new Append(Bytes.toBytes("haha")).add(
    //        Bytes.toBytes("f1"),
    //        Bytes.toBytes("test"),
    //        Bytes.toBytes(" world")
    //      ))
    //    }
    //    withMutator(conf, "test") { mutator =>
    //      mutator.mutate(new Delete(Bytes.toBytes("haha")).addColumns(
    //        Bytes.toBytes("f1"),
    //        Bytes.toBytes("test")
    //      ))
    //    }
  }

  @Test
  def withBufferedSinkTableTest(): Unit = {
    for (i <- 0 until 4) {
      withBufferedSinkTable(conf, "test") { table =>
        for (j <- 0 until 2000) {
          table.add(new Put(Bytes.toBytes(s"$i-$j")).addColumn(
            Bytes.toBytes("f1"),
            Bytes.toBytes("test"),
            Bytes.toBytes("hello")
          ))
          if (j % 2 == 0)
            table.add(new Delete(Bytes.toBytes(s"$i-$j")).addColumns(
              Bytes.toBytes("f1"),
              Bytes.toBytes("test")
            ))
        }
      }
    }
  }

  @Test
  def withSynchronizedBufferedSinkTableTest(): Unit = {
    withSynchronizedBufferedSinkTable(conf, "test") { table =>
      class Runner(id: Int) extends Runnable {
        override def run(): Unit = {
          for (i <- 0 until 1000) {
            val key = Bytes.toBytes(s"${Thread.currentThread().getName}-$id:$i")
            val family = Bytes.toBytes("f1")
            val column = Bytes.toBytes("bst")
            val value = Bytes.toBytes("gaga")
            table.add(new Put(key).addColumn(family, column, value))
            if (i % 2 == 0)
              table.add(new Delete(key).addColumns(family, column))
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
