package spark.streaming.potato.source.kafka.offsets

import kafka.common.InvalidConfigException
import kafka.consumer.ConsumerConfig
import spark.streaming.potato.source.kafka.KafkaSourceConstants

import scala.collection.mutable

import KafkaSourceConstants.{KAFKA_SOURCE_PREFIX => CONF_PREFIX}

class OffsetsManagerConf(sparkConf: Map[String, String], kafkaParams: Map[String, String] = Map.empty) {
  private val requiredKey = Set(
    OffsetsManagerConf.BOOTSTRAP_SERVERS_KEY,
    OffsetsManagerConf.GROUP_ID_KEY,
    OffsetsManagerConf.SUBSCRIBE_TOPICS_KEY,
    OffsetsManagerConf.OFFSETS_STORAGE_KEY,
    OffsetsManagerConf.OFFSET_RESET_POLICY
  )
  private val mapKey = Map(
    OffsetsManagerConf.OFFSET_RESET_POLICY -> Set("earliest" -> "smallest", "latest" -> "largest")
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
      OffsetsManagerConf.CONSUMER_CONFIG_PREFIX + conf._1 -> conf._2
    }

    val ret = Map(OffsetsManagerConf.CONSUMER_CONFIG_PREFIX + "zookeeper.connect" -> "") ++ sparkConf ++ tmpConf ++ tmpKafkaParams

    requiredKey.foreach { k =>
      if (!ret.contains(k))
        throw new InvalidConfigException(s"configuration $k not found.")
    }

    ret
  }

  val bootstrapServers: String = cleanedConf(OffsetsManagerConf.BOOTSTRAP_SERVERS_KEY)
  val groupId: String = cleanedConf(OffsetsManagerConf.GROUP_ID_KEY)
  val subscribeTopics: Set[String] = cleanedConf(OffsetsManagerConf.SUBSCRIBE_TOPICS_KEY)
    .split(",").map(_.trim).toSet
  val storageType: String = cleanedConf(OffsetsManagerConf.OFFSETS_STORAGE_KEY)
  val offsetResetPolicy: String = cleanedConf(OffsetsManagerConf.OFFSET_RESET_POLICY)
  val offsetsAutoUpdate: Boolean = cleanedConf.getOrElse(
    OffsetsManagerConf.OFFSETS_AUTO_UPDATE_KEY, OffsetsManagerConf.OFFSETS_AUTO_UPDATE_DEFAULT).toBoolean
  val offsetsAutoUpdateDelay: Long = cleanedConf.getOrElse(
    OffsetsManagerConf.OFFSETS_AUTO_UPDATE_DELAY_KEY, OffsetsManagerConf.OFFSETS_AUTO_UPDATE_DELAY_DEFAULT).toLong
  val consumerConfigs: Map[String, String] = cleanedConf.filter {
    _._1.startsWith(OffsetsManagerConf.CONSUMER_CONFIG_PREFIX)
  }.map { conf =>
    conf._1.substring(OffsetsManagerConf.CONSUMER_CONFIG_PREFIX.length) -> conf._2
  }
}

object OffsetsManagerConf {
  val OFFSETS_STORAGE_KEY: String = CONF_PREFIX + "offsets.storage"
  val SUBSCRIBE_TOPICS_KEY: String = CONF_PREFIX + "subscribe.topics"
  val OFFSETS_AUTO_UPDATE_KEY: String = CONF_PREFIX + "offsets.auto.update"
  val OFFSETS_AUTO_UPDATE_DEFAULT: String = "true"
  val OFFSETS_AUTO_UPDATE_DELAY_KEY: String = CONF_PREFIX + "offsets.auto.update.delay"
  val OFFSETS_AUTO_UPDATE_DELAY_DEFAULT = "0"

  val CONSUMER_CONFIG_PREFIX: String = CONF_PREFIX + "consumer."
  val GROUP_ID_KEY: String = CONSUMER_CONFIG_PREFIX + "group.id"
  val OFFSET_RESET_POLICY: String = CONSUMER_CONFIG_PREFIX + "auto.offset.reset"
  val BOOTSTRAP_SERVERS_KEY: String = CONSUMER_CONFIG_PREFIX + "bootstrap.servers"

  implicit def toMap(config: OffsetsManagerConf): Map[String, String] = {
    config.cleanedConf
  }

  implicit def toConsumerConfig(config: OffsetsManagerConf): ConsumerConfig = {
    import spark.streaming.potato.source.kafka.utils.OffsetsUtilImplicits.mapToProperties
    new ConsumerConfig(config.consumerConfigs)
  }
}
