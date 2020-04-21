#!/bin/echo

module_main_class=spark.potato.hadoop.cmd.FileMergeCmd

module_usage() {
  cat <<EOF
Usage:
  $(basename "$0") -p <potato_conf_file> -m filemerge <args>
  args:
    --source       ->  source directory.
    --target       ->  target directory, if not specified, use source directory.
    --format       ->  file format.
    --where-expr   ->  expression append to  sql 'where' clause.
    --compression  ->  compression used when writing file.
EOF
}

export_module_params() {
  export_prop spark.potato.submit.bin submit_bin
}

export_module_main_jar() {
  module_main_jar="$(find "$POTATO_LIB_DIR" -name "potato-hadoop-*.jar")"
  export module_main_jar
}

module_submit() {
  $submit_bin --properties-file "$potato_conf_file" \
    --deploy-mode client \
    --jars "$global_jars" \
    --class $module_main_class \
    "$module_main_jar" "$@"
}

do_work() {
  export_global_jars
  export_module_params
  export_module_main_jar
  module_submit "$@"
}
