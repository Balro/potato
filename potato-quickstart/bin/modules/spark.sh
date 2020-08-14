#!/bin/echo

export module_name="spark"
export POTATO_MAIN_JAR=$POTATO_HOME/lib/potato-spark-0.2.0-SNAPSHOT.jar

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") spark <opts> [main jar args]

opts:
  --lock <args>      manage the spark lock.
  --conf <key=value>   additional spark conf.
EOF
}

module_run() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "--lock")
      export POTATO_MAIN_CLASS="potato.spark.cmd.SingletonLockCli"
      export SPARK_ARGS="$SPARK_ARGS --master local[*]"
      ;;
    "--conf")
      shift
      export POTATO_SPARK_CONF="$POTATO_SPARK_CONF --conf $1"
      ;;
    *)
      break
      ;;
    esac
    shift
  done

  append_dep_jars "$POTATO_LIB_DIR"

  (test "$POTATO_MAIN_CLASS" && potato_submit "$@") || module_usage
}
