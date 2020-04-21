#!/usr/bin/env bash

usage() {
  cat <<EOF
Usage:
  $(basename "$0") [opts] <module> [module_args]

  opts:
    -h,--help  ->  show module usage

  module:
    submit     ->  submit app to cluster.
    lock       ->  manage app lock.
    offsets    ->  manage kafka offsets.
    filemerge  ->  hdfs file merge util.
EOF
}

source_env() {
  test -f /etc/profile && source /etc/profile
  test -f ~/.bash_profile && source ~/.bash_profile

  if [ -z "$POTATO_HOME" ]; then
    POTATO_HOME=$(cd -P "$(dirname "$0")/../" && pwd || exit)
    export POTATO_HOME
  fi
  export POTATO_BIN_DIR=$POTATO_HOME/bin
  export POTATO_LIB_DIR=$POTATO_HOME/lib
  export POTATO_LOG_DIR=$POTATO_HOME/logs

  if command -v spark_submit; then
    spark_bin=spark_submit
  elif [ -x "$SPARK_HOME/bin/spark-submit" ]; then
    spark_bin=$SPARK_HOME/bin/spark-submit
  else
    echo "spark-submit not found" >&2
    exit 1
  fi

  export spark_bin
}

# export_prop <conf_file> <conf_key> <key_name> [default_value] -> prop_key=prop_value|default_value
export_prop() {
  eval "local ${3}_=\"$(grep "^$2=" "$1" | tail -n 1 | awk -F '=' '{print $2}')\""
  test "$(eval echo \$"${3}"_)" && {
    eval export "$3"=\$"${3}"_
    return
  }
  test "$4" && {
    export "$3"="$4"
    return
  }
  echo "prop $3 not found in $1"
  exit 1
}

export_dep_jars() {
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
  export dep_jars="$jars_"
}

select_module() {
  for m in "$POTATO_HOME"/bin/modules/*; do
    if grep -wq "module_name=$1" "$m"; then
      source "$m"
    fi
  done

  if [ -z "$module_name" ]; then
    echo "module $1 not found" >&2
    usage
    exit 1
  fi
}

argparse() {
  while [ $# -gt 0 ]; do
    case "$1" in
    "-h" | "--help")
      shift
      select_module "$1"
      potato_run="module_usage"
      ;;
    *)
      select_module "$1"
      shift
      potato_run="module_run"
      potato_args=()
      while [ $# -gt 0 ]; do
        potato_args=("${potato_args[@]}" "$1")
        shift
      done
      ;;
    esac
    shift
  done
}

main() {
  source_env
  argparse "$@"
  $potato_run "${potato_args[@]}"
}

main "$@"
