#!/bin/echo

export module_name="submit"

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") <-p|--properties-file \${prop-file}> submit <opts> [main jar args]

opts:
  -c|--class <class>   main class on spark submit.
  -j|--jar <jar>       main jar file on spark submit.

EOF
}

submit_app() {
  test "$POTATO_PROP_FILE" || {
    echo "No prop file specified." >&2
    return 1
  }

  append_lib_jars

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
    "-c" | "--class")
      shift
      export POTATO_MAIN_CLASS="$1"
      ;;
    "-j" | "--jar")
      shift
      export POTATO_MAIN_JAR="$1"
      ;;
    *)
      break
      ;;
    esac
    shift
  done
  if ! submit_app "$@"; then
    module_usage
    exit 1
  fi
}
