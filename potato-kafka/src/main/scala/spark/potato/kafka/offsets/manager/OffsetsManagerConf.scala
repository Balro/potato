package spark.potato.kafka.offsets.manager

import kafka.common.InvalidConfigException
import kafka.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import spark.potato.kafka.conf._

import scala.collection.mutable

/**
 * 参数解析类，用于供OffsetsManager使用。
 *
 * @param conf        基本参数。
 * @param kafkaParams 额外提供给Consumer的参数。
 */
class OffsetsManagerConf(conf: Map[String, String], kafkaParams: Map[String, String] = Map.empty) {
  /**
   * SparkConf的包装类，用于快速提取OffsetsManager所需参数。
   *
   * @param sparkConf   需要包装的SparkConf。
   * @param kafkaParams 额外提供给Consumer的参数。
   */
  def this(sparkConf: SparkConf, kafkaParams: Map[String, String]) = {
    this(sparkConf.getAll.toMap, kafkaParams)
  }

  // 必须key，如conf未提供，则报错。
  private val requiredKey = Set(
    POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY,
    POTATO_KAFKA_CONSUMER_GROUP_ID_KEY,
    POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY,
    POTATO_KAFKA_OFFSETS_STORAGE_KEY,
    POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY
  )

  // 自动进行转换的参数，会将给定参数根据值进行转换。
  private val mapKey = Map(
    // 兼容新版本重置策略，转换为旧版本重置策略可识别参数。
    POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY -> Set("earliest" -> "smallest", "latest" -> "largest")
  )

  // 对传入参数进行校验，检查是否已包含必须key，并对需要转换的key进行转换。
  private val cleanedConf = {
    val tmpConf = mutable.Map.empty[String, String]

    mapKey.foreach { k =>
      if (conf.contains(k._1))
        k._2.foreach { kv =>
          if (conf(k._1) == kv._1)
            tmpConf += k._1 -> kv._2
        }
    }

    val tmpKafkaParams = kafkaParams.map { conf =>
      POTATO_KAFKA_CONSUMER_CONF_PREFIX + conf._1 -> conf._2
    }

    val ret = Map(POTATO_KAFKA_CONSUMER_CONF_PREFIX + "zookeeper.connect" -> "") ++ conf ++ tmpConf ++ tmpKafkaParams

    requiredKey.foreach { k =>
      if (!ret.contains(k))
        throw new InvalidConfigException(s"configuration $k not found.")
    }

    ret
  }

  /**
   * 根据前缀获取子参数。
   *
   * @param prefix 参数前缀。
   */
  def subPrefixConf(prefix: String): Map[String, String] = {
    cleanedConf.filter(_._1.startsWith(prefix)).map { c =>
      c._1.substring(prefix.length) -> c._2
    }
  }

  val bootstrapServers: String = cleanedConf(POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY)
  val groupId: String = cleanedConf(POTATO_KAFKA_CONSUMER_GROUP_ID_KEY)
  val subscribeTopics: Set[String] = cleanedConf(POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY)
    .split(",").map(_.trim).toSet
  val storageType: String = cleanedConf(POTATO_KAFKA_OFFSETS_STORAGE_KEY)
  val offsetResetPolicy: String = cleanedConf(POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY)
  val offsetsAutoUpdate: Boolean = cleanedConf.getOrElse(
    POTATO_KAFKA_OFFSETS_AUTO_UPDATE_KEY, POTATO_KAFKA_OFFSETS_AUTO_UPDATE_DEFAULT).toBoolean
  val offsetsAutoUpdateDelay: Long = cleanedConf.getOrElse(
    POTATO_KAFKA_OFFSETS_AUTO_UPDATE_DELAY_KEY, POTATO_KAFKA_OFFSETS_AUTO_UPDATE_DELAY_DEFAULT).toLong
  val consumerConfigs: Map[String, String] = cleanedConf.filter {
    _._1.startsWith(POTATO_KAFKA_CONSUMER_CONF_PREFIX)
  }.map { conf =>
    conf._1.substring(POTATO_KAFKA_CONSUMER_CONF_PREFIX.length) -> conf._2
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
