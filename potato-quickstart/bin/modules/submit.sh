#!/bin/echo

module_name="submit"

module_usage() {
  cat <<EOF
Usage:
  potato.sh submit <opts> <app jar> [main jar args]

opts-required:
  -p|--prop-file       specify the property file to load.
opts-optional:
  -c|--class <class>   main class on spark submit.
  -l|--log [logfile]   use nohup and run spark-submit on the background.
                       default logfile is $POTATO_HOME/logs/$APP_NAME-%Y%m%d-%H%M%S.log .
  --conf <key=value>   additional spark conf.
EOF
}

find_main_class_on_prop_file() {
  export_prop "$prop_file" spark.potato.submit.main.class main_class not_found
  test "$main_class" == "not_found"
}

submit_app() {
  local spark_run="$SPARK_BIN"

  append_dep_jars "$POTATO_LIB_DIR" && spark_run="$spark_run --jars $DEP_JARS"

  test "$main_class" || find_main_class_on_prop_file && spark_run="$spark_run --class $main_class"

  test "$spark_conf" && spark_run="$spark_run $spark_conf"

  test -r "$prop_file" && spark_run="$spark_run --properties-file $prop_file" || {
    echo "prop_file $prop_file not available" >&2
    exit 1
  }

  test -r "$main_jar" && spark_run="$spark_run $main_jar" || {
    echo "main_jar $main_jar not available" >&2
    exit 1
  }

  if [ "$log_file" ]; then
    nohup $spark_run "$@" &>"$log_file" &
  else
    $spark_run "$@"
  fi
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
      prop_file="$1"
      ;;
    "-c" | "--class")
      shift
      main_class="$1"
      ;;
    "-l" | "--log")
      shift
      if [ "$1" ]; then
        log_file="$1"
      else
        log_file="$POTATO_HOME/logs/$APP_NAME-%Y%m%d-%H%M%S.log"
      fi
      ;;
    "--conf")
      shift
      spark_conf="$spark_conf --conf $1"
      ;;
    *)
      main_jar="$1"
      shift
      submit_app "$@"
      ;;
    esac
    shift
  done
}
