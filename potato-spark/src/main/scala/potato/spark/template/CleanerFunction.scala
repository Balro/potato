package potato.spark.template

trait CleanerFunction {

  import potato.common.utils.JVMCleanUtil._

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
