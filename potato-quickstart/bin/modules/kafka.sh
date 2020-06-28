#!/bin/echo

export module_name="kafka"
POTATO_MAIN_JAR=$(find "$POTATO_HOME"/lib/potato-kafka-*.jar | head -n 1)
export POTATO_MAIN_JAR

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") kafka <opts> [main jar args]

opts:
  --offset <args>      manage the kafka offset.
  --conf <key=value>   additional spark conf.
EOF
}

module_run() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "--offset")
      export POTATO_MAIN_CLASS="potato.kafka010.offsets.cmd.KafkaOffsetCli"
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
