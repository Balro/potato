#!/bin/echo "this shell should be sourced"

module_usage() {
  cat <<EOF
Usage:
    $(basename $0) <potato_conf_file> submit [main jar args]
EOF
}

export_module_params() {
  export_prop spark.potato.submit.bin submit_bin
  export_prop spark.potato.main.class main_class
  export_prop spark.potato.main.jar main_jar
}

export_module_main_jar() {
  test -f $POTATO_LIB_DIR/$main_jar && return
  test -f $POTATO_LIB_DIR/$(basename $POTATO_BASE_DIR).jar && export main_jar=$(basename $POTATO_BASE_DIR).jar && return
  echo "main_jar not found."
  exit 1
}

module_submit() {
  $submit_bin --properties-file $potato_conf_file \
    --jars $global_jars \
    --class $main_class \
    $POTATO_LIB_DIR/$main_jar "$@" \
    >$POTATO_LOG_DIR/$main_jar-$main_class-$(date +'%Y%m%d_%H%M%S').out 2>&1
}

do_work() {
  export_module_params
  export_global_jars
  export_module_main_jar
  module_submit "$@"
}
