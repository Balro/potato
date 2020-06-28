#!/bin/echo

export module_name="hive"
POTATO_MAIN_JAR=$(find "$POTATO_HOME"/lib/potato-hive-*.jar | head -n 1)
export POTATO_MAIN_JAR

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") hive <opts> [main jar args]

opts:
  --export <args>      export hive data.
  --conf <key=value>   additional spark conf.
EOF
}

module_run() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "--export")
      export POTATO_MAIN_CLASS="potato.hive.cmd.HiveExportCli"
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
