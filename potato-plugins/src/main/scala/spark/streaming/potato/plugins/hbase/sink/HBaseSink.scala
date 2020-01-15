package spark.streaming.potato.plugins.hbase.sink

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Append, Increment, Mutation}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import spark.streaming.potato.plugins.hbase.ConnectionCache


object HBaseSink {
  implicit def toMutationRDD(rdd: RDD[MutationAction]): MutationRdd = {
    new MutationRdd(rdd)
  }
}

class MutationRdd(rdd: RDD[MutationAction]) extends ConnectionCache with Logging {
  def saveToHBaseTable(conf: Configuration, table: String, batchSize: Int): Unit = {
    rdd.foreachPartition { part =>
      var count = 0
      withMutator(conf, table) { mutator =>
        withTable(conf, table) { tbl =>
          part.foreach { mutate =>
            mutate match {
              case MutationAction(MutationType.APPEND, mutation) =>
                tbl.append(mutation.asInstanceOf[Append])
              case MutationAction(MutationType.INCREMENT, mutation) =>
                tbl.increment(mutation.asInstanceOf[Increment])
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
