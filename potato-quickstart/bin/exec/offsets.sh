#!/bin/echo "this shell should be sourced"

module_main_class=spark.streaming.potato.plugins.kafka.offsets.OffsetsCmd

module_usage() {
  cat <<EOF
Usage:
  $(basename $0) <potato_conf_file> offsets <args>
  args:
    list  -> clear old lock to stop app.
    lag   -> show current lag.
    reset -> reset offsets to earliest or latest.
EOF
}

export_module_params() {
  export_prop spark.potato.submit.bin submit_bin
}

export_module_main_jar() {
  export module_main_jar=$(find $POTATO_LIB_DIR -name potato-plugins-*.jar)
}

module_submit() {
  $submit_bin --properties-file $potato_conf_file \
    --master local \
    --deploy-mode client \
    --jars $global_jars \
    --class $module_main_class \
    $module_main_jar "$@"
}

do_work() {
  export_global_jars
  export_module_params
  export_module_main_jar
  module_submit "$@"
}
