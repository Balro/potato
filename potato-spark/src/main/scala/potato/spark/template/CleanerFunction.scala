package potato.spark.template

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

trait CleanerFunction {

  import potato.common.utils.JVMCleanUtil._

  trait StopWhenShutdown[T] {
    /**
     * 将包装对象的清理方法注册到清理方法中，当jvm退出时自动调用。
     */
    def stopWhenShutdown: T
  }

  class StopContextWhenShutdown(sc: SparkContext) extends StopWhenShutdown[SparkContext] {
    override def stopWhenShutdown: SparkContext = {
      clean(s"Stop SparkContext $sc when shutdown.", sc.stop)
      sc
    }
  }

  class StopStreamingWhenShutdown(ssc: StreamingContext) extends StopWhenShutdown[StreamingContext] {
    override def stopWhenShutdown: StreamingContext = {
      clean(s"Stop StreamingContext $ssc when shutdown.", () => ssc.stop())
      ssc
    }
  }

  class StopSparkWhenShutdown(spark: SparkSession) extends StopWhenShutdown[SparkSession] {
    def stopWhenShutdown: SparkSession = {
      clean(s"Stop SparkSession $spark when shutdown.", spark.stop)
      spark
    }
  }

  implicit def _stopWhenShutdown(sc: SparkContext): StopWhenShutdown[SparkContext] = new StopContextWhenShutdown(sc)

  implicit def _stopWhenShutdown(ssc: StreamingContext): StopWhenShutdown[StreamingContext] = new StopStreamingWhenShutdown(ssc)

  implicit def _stopWhenShutdown(spark: SparkSession): StopWhenShutdown[SparkSession] = new StopSparkWhenShutdown(spark)

  /**
   * 注册清理方法，注册多个清理方法时不保证方法调用顺序。
   *
   * @param desc 清理方法名称。
   * @param f    方法体。
   */
  def clean(desc: String, f: () => Unit): Unit = cleanWhenShutdown(desc, f)

  /**
   * 注册清理方法，清理方法按cleanInOrder调用顺序调用。
   *
   * @param desc 清理方法名称。
   * @param f    方法体。
   */
  def cleanInOrder(desc: String, f: () => Unit): Unit = cleaner.addCleanFunc(desc, f)
}
