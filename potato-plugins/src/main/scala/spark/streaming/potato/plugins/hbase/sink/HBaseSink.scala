package spark.streaming.potato.plugins.hbase.sink

import org.apache.hadoop.hbase.client.{Mutation, Row}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import spark.streaming.potato.plugins.hbase.GlobalConnectionCache._
import spark.streaming.potato.plugins.hbase.HBaseImplicits.mapToConfiguration

import scala.collection.mutable.ListBuffer

class MutationRdd(rdd: RDD[MutationAction]) extends Logging with Serializable {
  def saveToHBaseTable(conf: Map[String, String], table: String, batchSize: Int = 100): Unit = {
    import scala.collection.JavaConversions.bufferAsJavaList
    rdd.foreachPartition { part =>
      var count = 0
      val tblBuffer = ListBuffer.empty[Row]
      withMutator(conf, table) { mutator =>
        withTable(conf, table) { tbl =>
          part.foreach { mutate =>
            mutate match {
              case MutationAction(MutationType.APPEND, mutation) =>
                tblBuffer += mutation
              case MutationAction(MutationType.INCREMENT, mutation) =>
                tblBuffer += mutation
              case MutationAction(MutationType.DELETE, mutation) =>
                mutator.mutate(mutation)
              case MutationAction(MutationType.PUT, mutation) =>
                mutator.mutate(mutation)
              case m: MutationAction =>
                logWarning(s"Uknown mutation $m")
            }
            count += 1
            if (count >= batchSize) {
              mutator.flush()
              tbl.batch(tblBuffer, new Array[AnyRef](tblBuffer.size))
              count = 0
            }
          }
        }
        mutator.flush()
      }
    }
  }
}

case class MutationAction(action: MutationType.Type, mutation: Mutation)

object MutationType extends Enumeration {
  type Type = Value
  val APPEND, DELETE, PUT, INCREMENT = Value
}
