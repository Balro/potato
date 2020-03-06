package spark.potato.template.conf

import spark.potato.common.conf.CommonConfigKeys.POTATO_PREFIX

object TemplateConfigKeys {
  val POTATO_TEMPLATE_PREFIX: String = POTATO_PREFIX + "template."
  val POTATO_TEMPLATE_ADDITIONAL_SERVICES_KEY: String = POTATO_TEMPLATE_PREFIX + "additional.services"
}
