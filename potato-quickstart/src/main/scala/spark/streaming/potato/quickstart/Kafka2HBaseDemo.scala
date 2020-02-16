package spark.streaming.potato.quickstart

import java.util.Date

import org.apache.hadoop.hbase.client.{Append, Increment, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spark.streaming.potato.plugins.hbase.HBaseConfigKeys._
import spark.streaming.potato.plugins.hbase.HBaseImplicits._
import spark.streaming.potato.plugins.hbase.sink.HBaseSinkImplicits._
import spark.streaming.potato.plugins.hbase.sink.{MutationAction, MutationType}
import spark.streaming.potato.plugins.kafka.source.offsets.OffsetsManager
import spark.streaming.potato.plugins.kafka.source.KafkaSource
import spark.streaming.potato.template.template.KafkaSourceTemplate

object Kafka2HBaseDemo extends KafkaSourceTemplate[String] {
  override def initKafka(ssc: StreamingContext): (DStream[String], OffsetsManager) =
    KafkaSource.valueDStream(ssc)

  override def doWork(args: Array[String]): Unit = {
    stream.foreachRDD { rdd =>
      rdd.flatMap { f =>
        f.split("\\s+")
      }.distinct().filter {
        _.length > 0
      }.flatMap { f =>
        Seq(
          MutationAction(MutationType.PUT, new Put(Bytes.toBytes(f)).addColumn(
            Bytes.toBytes("f1"),
            Bytes.toBytes("date"),
            Bytes.toBytes(new Date().toString)
          )),
          MutationAction(MutationType.APPEND, new Append(Bytes.toBytes("append:" + f)).add(
            Bytes.toBytes("f1"),
            Bytes.toBytes("date"),
            Bytes.toBytes(new Date().toString)
          )),
          MutationAction(MutationType.INCREMENT, new Increment(Bytes.toBytes("increment:" + f)).addColumn(
            Bytes.toBytes("f1"),
            Bytes.toBytes("date"),
            1
          ))
        )
      }.saveToHBase(conf.getAllWithPrefix(POTATO_HBASE_SITE_PREFIX).toMap, "test")
    }
  }
}
