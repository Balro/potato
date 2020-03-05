package spark.potato.common.cache

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions.mapAsScalaConcurrentMap

/**
 * 轻量级对象缓存,适用于可以通过唯一key定位，并且仅需要少量实例即可满足需求的场景。
 * 自行调用internalGetOrCreate与internalClose使用该类。
 *
 * @note 该特质非线程安全，如多线使用，请自行加锁或使用synchronized。
 *       多线程情况下，不同线程可以通过同一key获取同一对象，建议存储的值类型为线程安全类型。
 *       不提供remove与update方法，以免删除正在被其他线程使用的元素。
 *       不建议使用close方法，原因同remove。
 *       需要大量值的场景请勿使用此缓存。
 */
abstract class KeyedCacheBase[K, V] {
  private val cache = new ConcurrentHashMap[K, V]()

  /**
   * @param key        获取缓存的唯一key。
   * @param createFunc 若key不存在，则将该方法生成新值纳入缓存并返回。
   */
  protected def internalGetOrCreate(key: K)(createFunc: () => V): V = {
    cache.getOrElseUpdate(key, createFunc())
  }

  /**
   * 关闭所有元素，并清空缓存。
   *
   * @note 注意多线程问题，可能正在被其他线程使用的元素。
   */
  protected def internalClose(closeFunc: V => Unit): Unit = {
    cache.foreach { kv =>
      closeFunc(kv._2)
    }
    cache.clear()
  }

  def size(): Int = cache.size()
}
