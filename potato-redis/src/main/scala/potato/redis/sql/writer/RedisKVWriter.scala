package potato.redis.sql.writer

import org.apache.spark.sql.DataFrame
import potato.redis.sink.RedisSinkUtil
import potato.spark.sql.writer.PotatoDataSourceWriter
import redis.clients.jedis.HostAndPort

class RedisKVWriter(df: DataFrame, hosts: Set[String], timeout: Int, auth: String) extends PotatoDataSourceWriter {
  override def write(): Unit = {
    val entry = df.rdd.map { r =>
      if (r.length>2){
      RedisSinkUtil.StringEntry(r.get(0).toString, r.get(1).toString, r.getAs[Int](2)).asInstanceOf[RedisSinkUtil.RedisEntry]
      } else {
      RedisSinkUtil.StringEntry(r.get(0).toString, r.get(1).toString).asInstanceOf[RedisSinkUtil.RedisEntry]
      }
    }
    val nodes = hosts.map { h =>
      val hp = h.split(":")
      new HostAndPort(hp(0), hp(1).toInt)
    }
    RedisSinkUtil.sinkToRedisCluster(entry, nodes, auth, timeout)
  }
}
