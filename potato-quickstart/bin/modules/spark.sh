#!/bin/echo

export module_name="spark"
export POTATO_MAIN_JAR="$(find "$POTATO_HOME/lib/" -name potato-spark-\*.jar | head -n 1)"

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") spark <opts> [main jar args]

opts:
  --lock <args>      manage the spark lock.
EOF
}

module_run() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "--lock")
      export POTATO_MAIN_CLASS="potato.spark.cmd.SingletonLockCli"
      export SPARK_ARGS="$SPARK_ARGS --master local[*] --deploy-mode client"
      ;;
    *)
      break
      ;;
    esac
    shift
  done

  append_lib_jars

  if ! (test "$POTATO_MAIN_CLASS" && potato_submit "$@"); then
    module_usage
    exit 1
  fi
}
