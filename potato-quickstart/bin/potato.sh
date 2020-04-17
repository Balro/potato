#!/usr/bin/env bash
POTATO_BASE_DIR=$(
  cd -P "$(dirname "$0")/../" || exit
  pwd
)
export POTATO_BASE_DIR
export POTATO_BIN_DIR=$POTATO_BASE_DIR/bin
export POTATO_LIB_DIR=$POTATO_BASE_DIR/lib
export POTATO_LOG_DIR=$POTATO_BASE_DIR/logs
mkdir -p "$POTATO_LOG_DIR"

export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2

usage() {
  cat <<EOF
Usage:
  $(basename "$0") <opts> -- [module args]

  opts:
    -h,--help   <module>   ->  show module usage
    -m,--module <module>   ->  module to be launched
    -p,--prop <prop_file>  ->  properties file for spark-submit

  modules:
    submit     ->  submit app to cluster.
    lock       ->  manage app lock.
    offsets    ->  manage kafka offsets.
    filemerge  ->  hdfs file merge util.
EOF
}

source_env() {
  test -f /etc/profile && source /etc/profile
  test -f ~/.bash_profile && source ~/.bash_profile
}

export_prop() {
  eval "local ${2}_=\"$(grep "^$1=" "$potato_conf_file" | tail -n 1 | awk -F '=' '{print $2}')\""
  test "$(eval echo \$"${2}"_)" && {
    eval export "$2"=\$"${2}"_
    return
  }
  test "$3" && {
    export "$2"="$3"
    return
  }
  echo "prop $2 not found in $potato_conf_file"
  exit 1
}

export_global_jars() {
  local jars_=
  for f in "$POTATO_LIB_DIR"/*; do
    test -f "$f" && {
      test "$jars_" && jars_="$f,$jars_" || jars_="$f"
    }
  done
  test "$jars_" || {
    echo "jars not valid."
    exit 1
  }
  export global_jars="$jars_"
}

argparse() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "-m" | "--module")
      shift
      if [ "$1" ]; then
        export module_name="$1"
        echo "launch module $module_name"
      else
        echo "module must be specified" >&2
        usage >&2
        exit 1
      fi
      ;;
    "-p" | "--prop")
      shift
      if [ -f "$1" ]; then
        export potato_conf_file="$1"
        echo "use property file $potato_conf_file"
      else
        echo "conf file not valid" >&2
        usage >&2
        exit 1
      fi
      ;;
    "-h" | "--help")
      shift
      if [[ "$1" && ! "$1" =~ ^-.* ]]; then
        export module_name="$1"
      fi
      export show_usage=1
      ;;
    "--")
      shift
      export module_args="$*"
      echo "modulem args: $module_args"
      return
      ;;
    *)
      echo "unparsed args"
      ;;
    esac
    shift
  done
}

main() {
  source_env
  argparse "$@"

  local module_file=$POTATO_BIN_DIR/exec/$module_name.sh
  if [ -f "$module_file" ]; then
    source $module_file
    shift
    if [ -n "$show_usage" ] && [ "$show_usage" -gt 0 ]; then
      module_usage
      return
    fi
    do_work "$module_args" || module_usage
  else
    echo "module not found"
    usage
  fi
}

main "$@"
