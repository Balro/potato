#!/bin/echo

export module_name="submit"

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") submit <opts> [main jar args]

opts-required:
  -p|--prop-file       specify the property file to load.
opts-optional:
  -c|--class <class>   main class on spark submit.
  -j|--jar <jar>       main jar file on spark submit.
  --conf <key=value>   additional spark conf.
EOF
}

find_main_class_on_prop_file() {
  export_prop "$prop_file" spark.potato.main.class main_class
}

find_main_jar_on_lib_dir() {
  local base_dir="$POTATO_HOME"
  test "$POTATO_CLIENT_HOME" && base_dir="$POTATO_CLIENT_HOME"
  export_prop "$prop_file" spark.potato.main.jar jar_name "not_found"
  test "$jar_name" == "not_found" && jar_name="$(basename "$base_dir").jar"
  test -r "$base_dir/lib/$jar_name" && export main_jar="$base_dir/lib/$jar_name" || {
    echo "main jar not found" >&2
    exit 1
  }
}

submit_app() {
  local spark_run="$SPARK_BIN"

  test -r "$prop_file" && spark_run="$spark_run --properties-file $prop_file" || {
    echo "prop_file $prop_file not available" >&2
    exit 1
  }

  append_dep_jars "$POTATO_LIB_DIR" && spark_run="$spark_run --jars $DEP_JARS"

  # main_class优先级，-c参数 > 配置文件的spark.potato.main.class参数 > main_jar的Meta值。
  test "$main_class" || find_main_class_on_prop_file && spark_run="$spark_run --class $main_class"

  if [ "$main_jar" ]; then
    test -r "$main_jar" || {
      echo "main_jar $main_jar not available" >&2
      exit 1
    }
  else
    find_main_jar_on_lib_dir
  fi
  spark_run="$spark_run $main_jar"

  test "$spark_conf" && spark_run="$spark_run $spark_conf"

  $spark_run "$@"
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
    "-j" | "--jar")
      shift
      main_jar="$1"
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
