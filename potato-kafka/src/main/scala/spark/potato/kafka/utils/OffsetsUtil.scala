package spark.potato.kafka.utils

import java.nio.channels.ClosedChannelException
import java.util.Properties

import kafka.api._
import kafka.cluster.BrokerEndPoint
import kafka.common._
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.utils.Logging
import org.apache.kafka.common.protocol.Errors

import scala.collection.mutable
import scala.util.Random

object OffsetsUtil extends Logging {
  val unknownBrokerId: Int = -1
  val invalidOffset: Long = -1L

  def commitOffsetsOnKafka(seeds: Set[BrokerEndPoint], groupId: String, offsets: Map[TopicAndPartition, Long], version: Short = 2)
                          (implicit config: ConsumerConfig): (Boolean, Seq[Throwable]) = {
    if (version != 1 && version != 2)
      throw new InvalidConfigException(s"$version is not a valid version, please use 1 or 2")
    commitOffsets(seeds, groupId, offsets, 2)
  }

  def commitOffsetsOnZookeeper(seeds: Set[BrokerEndPoint], groupId: String, offsets: Map[TopicAndPartition, Long])
                              (implicit config: ConsumerConfig): (Boolean, Seq[Throwable]) = {
    commitOffsets(seeds, groupId, offsets, 0)
  }

  def commitOffsets(seeds: Set[BrokerEndPoint], groupId: String, offsets: Map[TopicAndPartition, Long], version: Short)
                   (implicit config: ConsumerConfig): (Boolean, Seq[Throwable]) = {
    val errs = mutable.ArrayBuffer[Throwable]()
    withCoordinator(seeds, groupId, config) { consumer =>
      val req = OffsetCommitRequest(groupId, offsets.map { offset =>
        offset._1 -> OffsetAndMetadata(offset._2)
      }, version)
      val resp: OffsetCommitResponse = consumer.commitOffsets(req)
      if (!resp.hasError) return true -> errs
      else resp.commitStatus.filter {
        _._2 != 0
      }.foreach { e =>
        var err = ErrorMapping.exceptionFor(e._2)
        if (err.isInstanceOf[UnknownException])
          err = Errors.forCode(e._2).exception()
        errs += err
        warn(e._1 + " -> " + Errors.forCode(e._2).message())
      }
    }
    false -> errs
  }

  def validatedOrResetOffsets(seeds: Set[BrokerEndPoint], offsets: Map[TopicAndPartition, Long], reset: Boolean = true)
                             (implicit config: ConsumerConfig): Map[TopicAndPartition, Long] = {
    val taps = offsets.keySet
    val earliest = getEarliestOffsets(seeds, taps)
    val latest = getLatestOffsets(seeds, taps)
    offsets.map { offset =>
      offset._1 -> {
        offset._2 match {
          case o if o < earliest(offset._1) || o > latest(offset._1) =>
            if (reset) {
              config.autoOffsetReset match {
                case OffsetRequest.SmallestTimeString => earliest(offset._1)
                case OffsetRequest.LargestTimeString => latest(offset._1)
                case value => throw new InvalidConfigException(s"Wrong value $value of auto.offset.reset in ConsumerConfig; " +
                  s"Valid values are ${OffsetRequest.SmallestTimeString} and ${OffsetRequest.LargestTimeString}")
              }
            } else {
              throw new OffsetOutOfRangeException(s"${offset._1} -> ${offset._2} on [${earliest(offset._1)}, ${latest(offset._1)}]")
            }
          case o => o
        }
      }
    }
  }

  def fetchOffsetsOnKafka(seeds: Set[BrokerEndPoint], groupId: String, taps: Set[TopicAndPartition])
                         (implicit config: ConsumerConfig): Map[TopicAndPartition, Long] = {
    fetchOffsets(seeds, groupId, taps, 1)
  }

  def fetchOffsetsOnZookeeper(seeds: Set[BrokerEndPoint], groupId: String, taps: Set[TopicAndPartition])
                             (implicit config: ConsumerConfig): Map[TopicAndPartition, Long] = {
    fetchOffsets(seeds, groupId, taps, 0)
  }


  def fetchOffsets(seeds: Set[BrokerEndPoint], groupId: String, taps: Set[TopicAndPartition], version: Short)
                  (implicit config: ConsumerConfig): Map[TopicAndPartition, Long] = {
    withCoordinator(seeds, groupId, config) { consumer =>
      val req = OffsetFetchRequest(groupId, taps.toSeq, version)
      val resp = consumer.fetchOffsets(req)
      return resp.requestInfo.map { ri =>
        ri._2.error match {
          case ErrorMapping.NoError => ri._1 -> ri._2.offset
          case ErrorMapping.UnknownTopicOrPartitionCode if version == 0 =>
            warn(s"GroupId $groupId on ${ri._1} not found on zookeeper, use $invalidOffset instead.")
            ri._1 -> invalidOffset
          case err: Short =>
            throw new KafkaException(s"FetchOffsets for $groupId on ${ri._1} failed because: ${Errors.forCode(err).message()}")
        }
      }
    }
    throw new KafkaException(s"Cannot fetchOffsets for $groupId on $taps")
  }

  def withCoordinator[R](seeds: Set[BrokerEndPoint], groupId: String, config: ConsumerConfig)(f: SimpleConsumer => R): Map[Int, Option[R]] = {
    withBroker(seeds, config) { consumer =>
      val gcResp = consumer.send(GroupCoordinatorRequest(groupId, 0))
      if (gcResp.coordinatorOpt.isDefined) {
        return withBroker(Set(gcResp.coordinatorOpt.get), config)(f)
      }
    }
    throw new NotCoordinatorForConsumerException(s"Not coordinator for $groupId")
  }

  def getLatestOffsets(seeds: Set[BrokerEndPoint], taps: Set[TopicAndPartition])
                      (implicit config: ConsumerConfig): Map[TopicAndPartition, Long] = {
    getOffsetsBefore(seeds, taps, OffsetRequest.LatestTime)(config)
  }

  def getEarliestOffsets(seeds: Set[BrokerEndPoint], taps: Set[TopicAndPartition])
                        (implicit config: ConsumerConfig): Map[TopicAndPartition, Long] = {
    getOffsetsBefore(seeds, taps, OffsetRequest.EarliestTime)(config)
  }

  def getOffsetsBefore(seeds: Set[BrokerEndPoint], taps: Set[TopicAndPartition], time: Long)
                      (implicit config: ConsumerConfig): Map[TopicAndPartition, Long] = {
    val leaders = findLeaders(seeds, taps).groupBy(_._2).map { bt => bt._1 -> bt._2.keySet }

    leaders.flatMap { bt =>
      val requestInfo = bt._2.map { tap =>
        tap -> PartitionOffsetRequestInfo(time, 1)
      }.toMap
      val request = OffsetRequest(requestInfo)

      def getOffsets: Map[TopicAndPartition, Long] = {
        withBroker(Set(bt._1), config) { consumer =>
          val response = consumer.getOffsetsBefore(request)
          if (!response.hasError)
            return response.partitionErrorAndOffsets.map { peao =>
              peao._1 -> peao._2.offsets.head
            }
        }
        throw new KafkaException(s"Cannot found offsets for $taps")
      }

      getOffsets
    }
  }

  def getTopicAndPartitions(seeds: Set[BrokerEndPoint], topics: Set[String])
                           (implicit config: ConsumerConfig): Set[TopicAndPartition] = {
    getMetadata(seeds, topics).topicsMetadata.filter { tm => topics.contains(tm.topic) }.flatMap { tm =>
      tm.partitionsMetadata.map { pm => TopicAndPartition(tm.topic, pm.partitionId) }.toSet
    }.toSet
  }

  def findLeaders(seeds: Set[BrokerEndPoint], tps: Set[TopicAndPartition])
                 (implicit config: ConsumerConfig): Map[TopicAndPartition, BrokerEndPoint] = {
    val ret = mutable.Map.empty[TopicAndPartition, BrokerEndPoint]
    val tms = getMetadata(seeds).topicsMetadata.groupBy(_.topic)

    tps.groupBy(_.topic).foreach { ttap =>
      ttap._2.foreach { tap =>
        tms.get(ttap._1).foreach { topicMetadatas =>
          topicMetadatas.foreach { topicMetadata =>
            topicMetadata.partitionsMetadata.foreach { pm =>
              if (pm.partitionId == tap.partition && pm.leader.isDefined) {
                ret += tap -> pm.leader.get
              }
            }
          }
        }
      }
    }

    val notFound = tps.diff(ret.keys.toSet)

    if (notFound.isEmpty)
      return ret.toMap

    throw new LeaderNotAvailableException(s"Leader not found for: $notFound")
  }

  def findLeader(seeds: Set[BrokerEndPoint], tp: TopicAndPartition)(implicit config: ConsumerConfig): BrokerEndPoint = {
    getMetadata(seeds).topicsMetadata.foreach { tm =>
      if (tm.topic == tp.topic)
        tm.partitionsMetadata.foreach { pm: PartitionMetadata =>
          if (pm.partitionId == tp.partition && pm.leader.isDefined)
            return pm.leader.get
        }
    }
    throw new LeaderNotAvailableException(s"Leader not found for: $tp")
  }


  def findBrokers(seeds: Set[BrokerEndPoint])(implicit config: ConsumerConfig): Set[BrokerEndPoint] = {
    getMetadata(seeds).brokers.toSet
  }

  def getMetadata(seeds: Set[BrokerEndPoint], topics: Set[String] = Set.empty)
                 (implicit config: ConsumerConfig): TopicMetadataResponse = {
    var found = false
    withBroker(seeds, config) { consumer =>
      val res = consumer.send(TopicMetadataRequest(
        TopicMetadataRequest.CurrentVersion, 0, null, topics.toSeq))

      checkMeta()

      def checkMeta(): Unit = {
        res.topicsMetadata.foreach { tm =>
          if (tm.errorCode == ErrorMapping.NoError) {
            tm.partitionsMetadata.foreach { pm =>
              if (pm.errorCode != ErrorMapping.NoError) {
                warn(s"$pm -> ${Errors.forCode(pm.errorCode).message()}")
                return
              }
            }
            found = true
          } else {
            warn(s"${consumer.host}:${consumer.port} -> $tm")
            return
          }
        }
      }

      if (found)
        return res
    }
    throw MetadataNotFoundException(s"Cannot found valid metadata.")
  }

  def withBroker[R](brokers: Set[BrokerEndPoint], config: ConsumerConfig)
                   (f: SimpleConsumer => R): Map[Int, Option[R]] = {
    Random.shuffle(brokers).map { broker =>
      var consumer: SimpleConsumer = null
      try {
        consumer = new SimpleConsumer(
          broker.host, broker.port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)
        broker.id -> Option(f(consumer))
      } catch {
        case e: ClosedChannelException =>
          warn(s"Invalid broker: $broker -> ${e.getMessage}")
          broker.id -> None
      } finally {
        if (consumer != null)
          consumer.close()
      }
    }
  }.toMap

}

object OffsetsUtilImplicits {
  implicit val defaultConfig: ConsumerConfig = {
    val props = new Properties()
    props.setProperty("zookeeper.connect", "")
    props.setProperty("group.id", "")
    new ConsumerConfig(props)
  }

  implicit def mapToBrokerEndPoints(map: Map[String, Int]): Set[BrokerEndPoint] = {
    map.map { m =>
      BrokerEndPoint(OffsetsUtil.unknownBrokerId, m._1, m._2)
    }.toSet
  }

  implicit def mapToTopicAndPartitions(map: Map[String, Int]): Set[TopicAndPartition] = {
    map.map { m =>
      TopicAndPartition(m._1, m._2)
    }.toSet
  }

  implicit def mapToProperties(map: Map[String, String]): Properties = {
    import scala.collection.JavaConversions.propertiesAsScalaMap
    val props = new Properties()
    props ++= map
    props
  }

  implicit def mapToConsumerConfig(map: Map[String, String]): ConsumerConfig = {
    import scala.collection.JavaConversions.propertiesAsScalaMap
    val props = new Properties()
    props.setProperty("zookeeper.connect", "")
    props.setProperty("group.id", "")
    props ++= map
    new ConsumerConfig(props)
  }

  implicit def consumerConfigToRich(config: ConsumerConfig): RichConsumerConfig = {
    new RichConsumerConfig(config)
  }

  class RichConsumerConfig(config: ConsumerConfig) {
    def +(tuple: (String, String)): ConsumerConfig = {
      import scala.collection.JavaConversions.propertiesAsScalaMap
      new ConsumerConfig(mapToProperties((config.props.props + tuple).toMap))
    }

    def ++(map: Map[String, String]): ConsumerConfig = {
      import scala.collection.JavaConversions.propertiesAsScalaMap
      new ConsumerConfig(mapToProperties((config.props.props ++ map).toMap))
    }
  }

}

case class MetadataNotFoundException(msg: String = null, throwable: Throwable = null) extends KafkaException(msg, throwable)
