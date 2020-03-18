package spark.potato.common.service

trait GeneralService extends Service {
  def serve(conf: Map[String, String]): GeneralService
}
