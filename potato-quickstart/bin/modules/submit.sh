#!/bin/echo

export module_name="submit"

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") submit <opts> [main jar args]

opts-required:
  -p|--prop-file       specify the property file to load.
opts-optional:
  -c|--class <class>   main class on spark submit.
  -j|--jar <jar>       main jar file on spark submit.
  --conf <key=value>   additional spark conf.
EOF
}

submit_app() {
  test "$POTATO_PROP_FILE" || {
    echo "No prop file specified." >&2
    return 1
  }

  append_all_jars

  test "$POTATO_MAIN_CLASS" || export_main_class_from_prop_file
  test "$POTATO_MAIN_JAR" || export_main_jar_from_prop_file || export_main_jar_from_dir

  potato_submit "$@"
}

module_run() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "-h" | "--help")
      module_usage
      exit 0
      ;;
    "-p" | "--prop-file")
      shift
      export POTATO_PROP_FILE="$1"
      ;;
    "-c" | "--class")
      shift
      export POTATO_MAIN_CLASS="$1"
      ;;
    "-j" | "--jar")
      shift
      export POTATO_MAIN_JAR="$1"
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
  submit_app "$@" || module_usage
}
