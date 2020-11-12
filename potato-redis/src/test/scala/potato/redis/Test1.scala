package potato.redis

import org.junit.Test
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

object Test1 {
  def main(args: Array[String]): Unit = {
    import scala.collection.JavaConversions.setAsJavaSet
    val nodes = Set(new HostAndPort("10.111.149.43", 16379))
    val jedisCluster = new JedisCluster(nodes)
    println(jedisCluster.get("test"))
    jedisCluster.close()
  }

  @Test
  def clientTest(): Unit = {
    val jedis = new Jedis("10.111.149.43", 16379)
    jedis.auth("test1234")
    println(jedis.dbSize())
    jedis.close()
  }
}
