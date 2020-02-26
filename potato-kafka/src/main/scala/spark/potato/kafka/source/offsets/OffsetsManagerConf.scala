package spark.potato.kafka.source.offsets

import kafka.common.InvalidConfigException
import kafka.consumer.ConsumerConfig
import spark.potato.kafka.KafkaConfigKeys._

import scala.collection.mutable
import scala.language.implicitConversions

class OffsetsManagerConf(sparkConf: Map[String, String], kafkaParams: Map[String, String] = Map.empty) {
  private val requiredKey = Set(
    KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY,
    KAFKA_CONSUMER_GROUP_ID_KEY,
    KAFKA_SUBSCRIBE_TOPICS_KEY,
    KAFKA_OFFSETS_STORAGE_KEY,
    KAFKA_CONSUMER_OFFSET_RESET_POLICY
  )
  private val mapKey = Map(
    KAFKA_CONSUMER_OFFSET_RESET_POLICY -> Set("earliest" -> "smallest", "latest" -> "largest")
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
      KAFKA_CONSUMER_CONFIG_PREFIX + conf._1 -> conf._2
    }

    val ret = Map(KAFKA_CONSUMER_CONFIG_PREFIX + "zookeeper.connect" -> "") ++ sparkConf ++ tmpConf ++ tmpKafkaParams

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

  val bootstrapServers: String = cleanedConf(KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY)
  val groupId: String = cleanedConf(KAFKA_CONSUMER_GROUP_ID_KEY)
  val subscribeTopics: Set[String] = cleanedConf(KAFKA_SUBSCRIBE_TOPICS_KEY)
    .split(",").map(_.trim).toSet
  val storageType: String = cleanedConf(KAFKA_OFFSETS_STORAGE_KEY)
  val offsetResetPolicy: String = cleanedConf(KAFKA_CONSUMER_OFFSET_RESET_POLICY)
  val offsetsAutoUpdate: Boolean = cleanedConf.getOrElse(
    KAFKA_OFFSETS_AUTO_UPDATE_KEY, KAFKA_OFFSETS_AUTO_UPDATE_DEFAULT).toBoolean
  val offsetsAutoUpdateDelay: Long = cleanedConf.getOrElse(
    KAFKA_OFFSETS_AUTO_UPDATE_DELAY_KEY, KAFKA_OFFSETS_AUTO_UPDATE_DELAY_DEFAULT).toLong
  val consumerConfigs: Map[String, String] = cleanedConf.filter {
    _._1.startsWith(KAFKA_CONSUMER_CONFIG_PREFIX)
  }.map { conf =>
    conf._1.substring(KAFKA_CONSUMER_CONFIG_PREFIX.length) -> conf._2
  }
}

object OffsetsManagerConf {
  implicit def toMap(config: OffsetsManagerConf): Map[String, String] = {
    config.cleanedConf
  }

  implicit def toConsumerConfig(config: OffsetsManagerConf): ConsumerConfig = {
    import spark.potato.kafka.utils.OffsetsUtilImplicits.mapToProperties
    new ConsumerConfig(config.consumerConfigs)
  }
}
