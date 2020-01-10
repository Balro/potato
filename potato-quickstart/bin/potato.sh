#!/usr/bin/env bash
export POTATO_BASE_DIR=$(
  cd -P $(dirname $0)/../
  pwd
)
export POTATO_BIN_DIR=$POTATO_BASE_DIR/bin
export POTATO_LIB_DIR=$POTATO_BASE_DIR/lib
export POTATO_LOG_DIR=$POTATO_BASE_DIR/logs
mkdir -p $POTATO_LOG_DIR

export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2

usage() {
  cat <<EOF
Usage:
    $(basename $0) <potato_conf_file> <service> [service args]

    services:
        submit  ->  submit app to cluster.
        lock    ->  manage app lock.
EOF
}

source_env() {
  test -f /etc/profile && source /etc/profile
  test -f ~/.bash_profile && source ~/.bash_profile
}

locate_conf() {
  test $# -gt 0 && test -f $1 && export potato_conf_file=$1 || {
    echo "conf file not valid." >&2
    usage >&2
    exit 1
  }
}

export_prop() {
  eval local ${2}_=\"$(grep "^$1" $potato_conf_file | tail -n 1 | awk -F '=' '{print $2}')\"
  test "$(eval echo \$${2}_)" && {
    eval export $2=\$${2}_
    return
  }
  test "$3" && {
    export $2="$3"
    return
  }
  echo "prop $2 not found in $potato_conf_file"
  exit 1
}

class_path() {
  local class_path_=""
  local lib_path=$1
  for f in $(ls $lib_path/); do
    test -f $lib_path/$f && {
      test "$class_path_" && class_path_=$lib_path/$f:$class_path_ || class_path_=$lib_path/$f
    }
  done
  echo $class_path_
}

main() {
  source_env
  locate_conf $1
  shift

  local source_file=$POTATO_BIN_DIR/exec/$1.sh
  test -f $source_file && {
    source $source_file
    shift
    do_work "$@" || service_usage
  } || {
    echo "service not found"
    usage
  }
}

main "$@"
