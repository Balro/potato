package spark.streaming.potato.plugins.hbase

import org.apache.hadoop.hbase.client.{Append, Delete, Increment, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test
import spark.streaming.potato.plugins.hbase.HBaseImplicits._
import spark.streaming.potato.plugins.hbase.GlobalConnectionCache._

class GlobalConnectionCacheTest {
  val conf = Map(
    "hbase.zookeeper.quorum" -> "test01,test02"
  )

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
    //    withMutator(conf, "test") { mutator =>
    //      mutator.mutate(new Delete(Bytes.toBytes("haha")).addColumns(
    //        Bytes.toBytes("f1"),
    //        Bytes.toBytes("test")
    //      ))
    //    }
  }

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
}
