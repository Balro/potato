package potato.kafka010.offsets.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import potato.hadoop.conf.SerializedConfiguration._
import spark.potato.hbase.table.TableUtil.withTable
import spark.potato.kafka.offsets.KafkaConsumerOffsetsUtil

/**
 * 建表语句参考
 * create 'kafka_offsets_storage',NAME => 'partition',VERSIONS => '5',IN_MEMORY => 'true', TTL => '604800'
 *
 * 备注:
 * 直接使用Bytes工具类包装long型，导致hbase shell可读性变差，故将long转String后再进行导入。
 *
 * @param conf 该参数应对应 hbase-site.xml 中的参数。必要参数 zk地址、端口等。
 */
class HBaseOffsetsStorage(table: String, family: String, conf: Map[String, String]) extends OffsetsStorage with Logging {
  val hbaseConf: Configuration = HBaseConfiguration.create(conf)

  /*
   修复hbase无法连接时等待时间过长的bug。
   时间仅供参考:
      6 -> 17s
      7 -> 37s
      8 -> 81s
      9 -> 192s
      10 -> 322s
   */
  hbaseConf.set("hbase.client.retries.number", "8")

  override def save(groupId: String, offsets: Map[TopicPartition, Long]): Boolean = {
    import scala.collection.JavaConversions.seqAsJavaList

    withTable(hbaseConf, table) { tbl =>
      val puts = offsets.groupBy(_._1.topic).map { ttpo =>
        val put = new Put(Bytes.toBytes(makeKey(groupId, ttpo._1)))
        ttpo._2.foreach { tapo =>
          put.addColumn(
            Bytes.toBytes(family),
            Bytes.toBytes(tapo._1.partition.toString),
            Bytes.toBytes(tapo._2.toString))
        }
        put
      }.toSeq
      try {
        tbl.batch(puts, new Array[AnyRef](puts.size))
      } catch {
        case e: Throwable =>
          logWarning("Save offsets to hbase err.", e)
          return false
      }
      true
    }
  }

  override def load(groupId: String, tps: Set[TopicPartition]): Map[TopicPartition, Long] = {
    import scala.collection.JavaConversions.collectionAsScalaIterable
    withTable(conf, table) { tbl =>
      val tapo = tps.groupBy { tap => tap.topic }.flatMap { ttap =>
        val get = new Get(Bytes.toBytes(makeKey(groupId, ttap._1)))
        ttap._2.foreach { tap =>
          get.addColumn(
            Bytes.toBytes(family),
            Bytes.toBytes(tap.partition.toString))
        }
        try {
          tbl.get(get).listCells().toSeq.map { cell =>
            new TopicPartition(ttap._1, Bytes.toString(CellUtil.cloneQualifier(cell)).toInt) ->
              Bytes.toString(CellUtil.cloneValue(cell)).toLong
          }
        } catch {
          case _: NullPointerException =>
            logWarning(s"Offset not found $ttap")
            Seq.empty[(TopicPartition, Long)]
        }
      }
      tps.map { tap =>
        tap -> KafkaConsumerOffsetsUtil.invalidOffset // 新partition默认赋予无效offset值。
      }.toMap ++ tapo
    }
  }

  def makeKey(groupId: String, topic: String): String = groupId + "##" + topic
}
