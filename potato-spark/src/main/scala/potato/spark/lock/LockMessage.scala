package potato.spark.lock

import org.apache.spark.SparkContext
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson._

class LockMessage(sc: SparkContext) {
  private implicit val fmt: Formats = DefaultFormats
  private val msg = Map(
    "appName" -> sc.appName,
    "applicationId" -> sc.applicationId,
    "applicationAttemptId" -> sc.applicationAttemptId.getOrElse("-1"),
    "deployMode" -> sc.deployMode,
    "master" -> sc.master,
    "startTime" -> sc.startTime.toString,
    "user" -> sc.sparkUser,
    "webUri" -> sc.uiWebUrl.getOrElse("null")
  )

  def toJsonString: String = compactJson(msg)

  override def toString: String = toJsonString
}
