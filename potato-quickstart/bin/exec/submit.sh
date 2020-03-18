#!/bin/echo

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") -p <potato_conf_file> -m submit [-s] [main jar args]
Args:
  -s  start app silently and log msg to logfile.
EOF
}

export_module_params() {
  export_prop spark.potato.submit.bin submit_bin
  export_prop spark.potato.submit.main.class main_class
  export_prop spark.potato.submit.main.jar main_jar
}

export_module_main_jar() {
  test -f "$POTATO_LIB_DIR/$main_jar" && return
  test -f "$POTATO_LIB_DIR/$(basename "$POTATO_BASE_DIR").jar" && main_jar=$(basename "$POTATO_BASE_DIR").jar && {
    export main_jar
    return
  }
  echo "main_jar not found."
  exit 1
}

module_submit() {
  $submit_bin --properties-file "$potato_conf_file" \
    --jars "$global_jars" \
    --class "$main_class" \
    "$POTATO_LIB_DIR/$main_jar" "$@"

}

do_work() {
  export_module_params
  export_global_jars
  export_module_main_jar

  if [ "$1" = "-s" ]; then
    shift
    export -f module_submit
    nohup bash -c "module_submit \"$*\"" >"$POTATO_LOG_DIR/$main_jar-$main_class-$(date +'%Y%m%d_%H%M%S').out" 2>&1 &
  else
    module_submit "$@"
  fi
}
