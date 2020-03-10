package spark.potato.template

import spark.potato.common.conf.POTATO_PREFIX

package object conf {
  val POTATO_TEMPLATE_PREFIX: String = POTATO_PREFIX + "template."
  val POTATO_TEMPLATE_ADDITIONAL_SERVICES_KEY: String = POTATO_TEMPLATE_PREFIX + "additional.services"
}
