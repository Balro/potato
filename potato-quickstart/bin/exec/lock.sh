#!/bin/echo "this shell should be sourced"

lock_jar=spark.streaming.potato.core.context.lock.RunningLockCmd

service_usage() {
  cat <<EOF
Usage:
  $(basename $0) <potato_conf_file> lock clear
  args:
    clear -> clear old lock to stop app.
EOF
}

do_work() {
  local cp=$(class_path $POTATO_LIB_DIR)

  scala -cp $cp $lock_jar "$@"
}
