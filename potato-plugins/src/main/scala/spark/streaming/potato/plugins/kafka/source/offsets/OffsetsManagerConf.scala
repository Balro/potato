package spark.streaming.potato.plugins.kafka.source.offsets

import kafka.common.InvalidConfigException
import kafka.consumer.ConsumerConfig
import spark.streaming.potato.plugins.kafka.KafkaConfigKeys._

import scala.collection.mutable
import scala.language.implicitConversions

class OffsetsManagerConf(sparkConf: Map[String, String], kafkaParams: Map[String, String] = Map.empty) {
  private val requiredKey = Set(
    CONSUMER_BOOTSTRAP_SERVERS_KEY,
    CONSUMER_GROUP_ID_KEY,
    SUBSCRIBE_TOPICS_KEY,
    OFFSETS_STORAGE_KEY,
    CONSUMER_OFFSET_RESET_POLICY
  )
  private val mapKey = Map(
    CONSUMER_OFFSET_RESET_POLICY -> Set("earliest" -> "smallest", "latest" -> "largest")
  )

  private val cleanedConf = {
    val tmpConf = mutable.Map.empty[String, String]

    mapKey.foreach { k =>
      if (sparkConf.contains(k._1))
        k._2.foreach { kv =>
          if (sparkConf(k._1) == kv._1)
            tmpConf += k._1 -> kv._2
        }
    }

    val tmpKafkaParams = kafkaParams.map { conf =>
      CONSUMER_CONFIG_PREFIX + conf._1 -> conf._2
    }

    val ret = Map(CONSUMER_CONFIG_PREFIX + "zookeeper.connect" -> "") ++ sparkConf ++ tmpConf ++ tmpKafkaParams

    requiredKey.foreach { k =>
      if (!ret.contains(k))
        throw new InvalidConfigException(s"configuration $k not found.")
    }

    ret
  }

  def subPrefixConf(prefix: String): Map[String, String] = {
    cleanedConf.filter(_._1.startsWith(prefix)).map { c =>
      c._1.substring(prefix.length) -> c._2
    }
  }

  val bootstrapServers: String = cleanedConf(CONSUMER_BOOTSTRAP_SERVERS_KEY)
  val groupId: String = cleanedConf(CONSUMER_GROUP_ID_KEY)
  val subscribeTopics: Set[String] = cleanedConf(SUBSCRIBE_TOPICS_KEY)
    .split(",").map(_.trim).toSet
  val storageType: String = cleanedConf(OFFSETS_STORAGE_KEY)
  val offsetResetPolicy: String = cleanedConf(CONSUMER_OFFSET_RESET_POLICY)
  val offsetsAutoUpdate: Boolean = cleanedConf.getOrElse(
    OFFSETS_AUTO_UPDATE_KEY, OFFSETS_AUTO_UPDATE_DEFAULT).toBoolean
  val offsetsAutoUpdateDelay: Long = cleanedConf.getOrElse(
    OFFSETS_AUTO_UPDATE_DELAY_KEY, OFFSETS_AUTO_UPDATE_DELAY_DEFAULT).toLong
  val consumerConfigs: Map[String, String] = cleanedConf.filter {
    _._1.startsWith(CONSUMER_CONFIG_PREFIX)
  }.map { conf =>
    conf._1.substring(CONSUMER_CONFIG_PREFIX.length) -> conf._2
  }
}

object OffsetsManagerConf {
  implicit def toMap(config: OffsetsManagerConf): Map[String, String] = {
    config.cleanedConf
  }

  implicit def toConsumerConfig(config: OffsetsManagerConf): ConsumerConfig = {
    import spark.streaming.potato.plugins.kafka.utils.OffsetsUtilImplicits.mapToProperties
    new ConsumerConfig(config.consumerConfigs)
  }
}
