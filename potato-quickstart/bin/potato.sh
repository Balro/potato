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

    modules:
        submit   ->  submit app to cluster.
        lock     ->  manage app lock.
        offsets  ->  manage offsets.
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

export_global_jars() {
  local jars_=
  for f in $(ls $POTATO_LIB_DIR/); do
    test -f $POTATO_LIB_DIR/$f && {
      test "$jars_" && jars_=$POTATO_LIB_DIR/$f,$jars_ || jars_=$POTATO_LIB_DIR/$f
    }
  done
  test "$jars_" || {
    echo "jars not valid."
    exit 1
  }
  export global_jars="$jars_"
}

main() {
  source_env
  locate_conf $1
  shift

  local source_file=$POTATO_BIN_DIR/exec/$1.sh
  test -f $source_file && {
    source $source_file
    shift
    do_work "$@" || module_usage
  } || {
    echo "module not found"
    usage
  }
}

main "$@"
