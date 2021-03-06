package potato.kafka08.offsets.storage

import kafka.common.TopicAndPartition
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.internal.Logging
import potato.kafka08.conf._

/**
 * 建表语句参考
 * create 'kafka_offsets_storage',NAME => 'partition',VERSIONS => '5',IN_MEMORY => 'true', TTL => '604800'
 *
 * 备注:
 * 直接使用Bytes工具类包装long型，导致hbase shell可读性变差，故将long转String后再进行导入。
 *
 * @param table 存取offset的hbase表。
 * @param conf  该参数应对应 hbase-site.xml 中的参数。主要参数 zk地址、端口等。
 */
class HBaseOffsetsStorage(table: String, conf: Map[String, String]) extends OffsetsStorage with Logging {
  val hbaseConf: Configuration = HBaseConfiguration.create()
  conf.foreach { c => hbaseConf.set(c._1, c._2) }

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
  val conn: Connection = ConnectionFactory.createConnection(hbaseConf)

  override def save(groupId: String, offsets: Map[TopicAndPartition, Long]): Boolean = {
    import scala.collection.JavaConversions.seqAsJavaList
    val tbl: Table = conn.getTable(TableName.valueOf(table))
    val puts = offsets.groupBy(_._1.topic).map { ttapo =>
      val put = new Put(Bytes.toBytes(makeKey(groupId, ttapo._1)))
      ttapo._2.foreach { tapo =>
        put.addColumn(
          Bytes.toBytes(conf.getOrElse(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_FAMILY_KEY, POTATO_KAFKA_OFFSETS_STORAGE_HBASE_FAMILY_DEFAULT)),
          Bytes.toBytes(tapo._1.partition.toString),
          Bytes.toBytes(tapo._2.toString))
      }
      put
    }.toSeq
    try {
      tbl.batch(puts, new Array[AnyRef](puts.size))
      tbl.close()
    } catch {
      case e: Throwable =>
        logWarning("Save offsets to hbase err.", e)
        return false
    }
    tbl.close()
    true
  }

  override def load(groupId: String, taps: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    import scala.collection.JavaConversions.collectionAsScalaIterable
    val tbl: Table = conn.getTable(TableName.valueOf(table))
    val tapo = taps.groupBy { tap => tap.topic }.flatMap { ttap =>
      val get = new Get(Bytes.toBytes(makeKey(groupId, ttap._1)))
      ttap._2.foreach { tap =>
        get.addColumn(
          Bytes.toBytes(conf.getOrElse(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_FAMILY_KEY, POTATO_KAFKA_OFFSETS_STORAGE_HBASE_FAMILY_DEFAULT)),
          Bytes.toBytes(tap.partition.toString))
      }
      try {
        tbl.get(get).listCells().toSeq.map { cell =>
          TopicAndPartition(ttap._1, Bytes.toString(CellUtil.cloneQualifier(cell)).toInt) ->
            Bytes.toString(CellUtil.cloneValue(cell)).toLong
        }

      } catch {
        case _: NullPointerException =>
          logWarning(s"Offset not found $ttap")
          Seq.empty[(TopicAndPartition, Long)]
      }
    }
    tbl.close()
    taps.map { tap =>
      tap -> -1L
    }.toMap ++ tapo
  }

  def makeKey(groupId: String, topic: String): String = groupId + "##" + topic
}
