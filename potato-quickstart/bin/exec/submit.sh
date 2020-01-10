#!/bin/echo "this shell should be sourced"

service_usage() {
  cat <<EOF
Usage:
    $(basename $0) <potato_conf_file> submit [main jar args]
EOF
}

mk_params() {
  export_prop spark.potato.submit.bin submit_bin
  export_prop spark.potato.main.class main_class
  export_prop spark.potato.main.jar main_jar
}

mk_jars() {
  local jars_=
  for f in $(ls $POTATO_LIB_DIR/); do
    test -f $POTATO_LIB_DIR/$f && {
      test "$jars_" && jars_=$POTATO_LIB_DIR/$f:$jars_ || jars_=$POTATO_LIB_DIR/$f
    }
  done
  test "$jars_" || {
    echo "jars not valid."
    exit 1
  }
  export jars="$jars_"
}

mk_main_jar() {
  test -f $POTATO_LIB_DIR/$main_jar && return
  test -f $POTATO_LIB_DIR/$(basename $POTATO_BASE_DIR).jar && export main_jar=$(basename $POTATO_BASE_DIR).jar && return
  echo "main_jar not found."
  exit 1
}

mk_submit() {
  $submit_bin --properties-file $potato_conf_file \
    --jars $jars \
    --class $main_class \
    $POTATO_LIB_DIR/$main_jar "$@" \
    >$POTATO_LOG_DIR/$main_jar-$main_class-$(date +'%Y%m%d_%H%M%S').out 2>&1
}

do_work() {
  mk_params
  mk_jars
  mk_main_jar
  mk_submit "$@"
}
