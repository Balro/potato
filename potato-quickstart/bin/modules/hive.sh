#!/bin/echo

export module_name="hive"
export POTATO_MAIN_JAR="$(find "$POTATO_HOME/lib/" -name potato-hive-\*.jar | head -n 1)"

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") hive <opts> [main jar args]

opts:
  --export <args>      export hive data.
EOF
}

module_run() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "--export")
      export POTATO_MAIN_CLASS="potato.hive.cmd.HiveExportCli"
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
