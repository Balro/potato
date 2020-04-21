#!/bin/echo

module_name=submit

module_usage() {
  cat <<EOF
Usage:
  potato.sh submit <opts> <app jar> [main jar args]

opts-required:
  -p|--prop-file       specify the property file to load.
opts-optional:
  -c|--class <class>   main class on spark submit.
  -l|--log [logfile]   logging to the logfile, default $POTATO_HOME/logs/$APP_NAME-yyyyMMdd-HHSS.log.
                       if not specified, logging to the console.
  --conf <key=value>   additional spark conf.
EOF
}

find_main_class() {
  export_prop "$prop_file" spark.potato.submit.main.class main_class not_found
  test "$main_class" == "not_found"
}

export_module_main_jar() {
  test -f "$POTATO_LIB_DIR/$main_jar" && return
  test -f "$POTATO_LIB_DIR/$(basename "$POTATO_BASE_DIR").jar" && main_jar=$(basename "$POTATO_BASE_DIR").jar && {
    export main_jar
    return
  }
  echo "main_jar not found."
  exit 1
}

submit_app() {
  spark_run="$spark_bin"

  if [ -r "$prop_file" ]; then
    spark_run="$spark_run --properties-file $prop_file"
  else
    echo "prop_file $prop_file not available" >&2
    exit 1
  fi

  export_dep_jars && spark_run="$spark_run --jars $dep_jars"
  test "$main_class" && spark_run="$spark_run --class $main_class"

  if [ "$main_class" ]; then
    spark_run="$spark_run --class $main_class"
  elif find_main_class; then
    spark_run="$spark_run --class $main_class"
  fi

  test "$spark_conf" && spark_run="$spark_run $spark_conf"

  if [ -r "$main_jar" ]; then
    spark_run="$spark_run $main_jar $main_args"
  else
    echo "main_jar $main_jar not available" >&2
    exit 1
  fi

  test "$log_file" && spark_run="$spark_run &> $log_file"

  $spark_run
}

module_argparse() {
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
      log_file="$1"
      ;;
    "--conf")
      shift
      spark_conf="$spark_conf --conf $1"
      ;;
    *)
      main_jar="$1"
      shift
      main_args="$*"
      ;;
    esac
    shift
  done

}

module_run() {
  module_args "$@"
  submit_app
}
