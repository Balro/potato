#!/bin/echo

export module_name="hadoop"

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") hadoop <opts>

opts:
  --file-merge <args>  file merge function.
  --conf <key=value>   additional spark conf.
EOF
}

module_run() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "--file-merge")
      main_class=
      exit 0
      ;;
    "--conf")
      shift
      spark_conf="$spark_conf --conf $1"
      ;;
    *)
      break
      ;;
    esac
    shift
  done
  submit_app "$@" || module_usage
}
