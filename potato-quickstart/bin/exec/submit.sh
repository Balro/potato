#!/bin/echo "this shell should be sourced"

service_usage() {
    cat << EOF
Usage:
    $(basename $0) <conf_file> submit [main jar args]
EOF
}

mk_params() {
    export_prop spark.potato.submit.bin submit_bin
    export_prop spark.potato.main.class main_class
    export_prop spark.potato.main.jar main_jar
}

mk_jars() {
    local jars_=
    for f in $(ls $BASE_DIR/lib/); do
        test -f $BASE_DIR/lib/$f && {
            test "$jars_" && jars_=$BASE_DIR/lib/$f,$jars_ || jars_=$BASE_DIR/lib/$f
        }
    done
    test "$jars_" || {
        echo "jars not valid."
        exit 1
    }
    export jars="$jars_"
}

mk_main_jar() {
    test -f $BASE_DIR/lib/$main_jar && return
    test -f $BASE_DIR/lib/$(basename $BASE_DIR).jar && export main_jar=$(basename $BASE_DIR).jar && return
    echo "main_jar not found."
    exit 1
}

mk_submit() {
    mkdir -p $BASE_DIR/logs
    $submit_bin --properties-file $conf_file \
        --jars $jars \
        --class $main_class \
        $BASE_DIR/lib/$main_jar "$@" \
        > $BASE_DIR/logs/$main_jar-$main_class-$(date +'%Y%m%d_%H%M%S').out 2>&1
}

do_work() {
    mk_params
    mk_jars
    mk_main_jar
    mk_submit "$@"
}
