package potato.redis.sink

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.rdd.RDD
import redis.clients.jedis.params.SetParams
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

import scala.collection.JavaConversions._

object RedisSinkUtil {

  class RedisEntry

  case class StringEntry(key: String, value: String, ttl: Int = -1) extends RedisEntry

  case class HashEntry(key: String, hash: Map[String, String], ttl: Int = -1) extends RedisEntry

  def sinkToRedis(rdd: RDD[RedisEntry], host: String, port: Int, auth: String): Unit = {
    rdd.foreachPartition { p =>
      val jedis = new Jedis(host, port)
      jedis.auth(auth)
      val pip = jedis.pipelined()
      p.foreach {
        case StringEntry(k, v, t) =>
          if (t <= 0) {
            pip.set(k, v)
          } else {
            pip.set(k, v, SetParams.setParams().ex(t))
          }
        case HashEntry(k, h, t) =>
          pip.hset(k, h)
          if (t > 0) {
            pip.expire(k, t)
          }
      }
      pip.sync()
      pip.close()
      jedis.close()
    }
  }

  def sinkToRedisCluster(rdd: RDD[RedisEntry], startNodes: Set[HostAndPort], auth: String, timeout: Int = 5000, attempt: Int = 5): Unit = {
    rdd.foreachPartition { p =>
      val cluster = new JedisCluster(startNodes, timeout, timeout, attempt, auth, new GenericObjectPoolConfig())
      p.foreach {
        case StringEntry(k, v, t) =>
          if (t <= 0) {
            cluster.set(k, v)
          } else {
            cluster.set(k, v, SetParams.setParams().ex(t))
          }
        case HashEntry(k, h, t) =>
          cluster.hset(k, h)
          if (t > 0) {
            cluster.expire(k, t)
          }
      }
      cluster.close()
    }

  }

}
