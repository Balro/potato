#!/bin/echo

export module_name="hadoop"
export POTATO_MAIN_JAR="$(find "$POTATO_HOME/lib/" -name potato-hadoop-\*.jar | head -n 1)"

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") hadoop <opts> [main jar args]

opts:
  --file-merge <args>  file merge function.
EOF
}

module_run() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "--file-merge")
      export POTATO_MAIN_CLASS="potato.hadoop.cmd.FileMergeCli"
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
