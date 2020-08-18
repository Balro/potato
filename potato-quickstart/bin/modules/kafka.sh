#!/bin/echo

export module_name="kafka"
export POTATO_MAIN_JAR="$(find "$POTATO_HOME/lib/" -name potato-kafka-\*.jar | head -n 1)"

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
      export POTATO_MAIN_CLASS="potato.kafka010.cmd.KafkaOffsetCli"
      export SPARK_ARGS="$SPARK_ARGS --master local[*] --deploy-mode client"
      ;;
    *)
      break
      ;;
    esac
    shift
  done

  append_lib_jars

  (test "$POTATO_MAIN_CLASS" && potato_submit "$@") || module_usage
}
