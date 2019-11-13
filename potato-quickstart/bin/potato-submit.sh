#!/usr/bin/env bash
BASE_DIR=`cd -P $(dirname $0)/../;pwd`
mkdir -p $BASE_DIR/logs

usage() {
    cat << EOF
$0 <conf_file>
EOF
}

source_env() {
    test -f /etc/profile && source /etc/profile
    test -f ~/.bash_profile && source ~/.bash_profile
}

locate_conf() {
    test $# -gt 0 && test -f $1 && export conf_file=$1 || {
        echo "conf file not valid." >&2
        usage >&2
        exit 1
    }
}

export_prop() {
    eval local ${2}_=\"`grep "^$1" $conf_file | tail -n 1 | awk -F '=' '{print $2}'`\"
    test "$(eval echo \$${2}_)" &&  {
        eval export $2=\$${2}_
        return
    }
    test "$3" && {
        export $2="$3"
        return
    }
    echo "prop $2 not found in $conf_file"
    exit 1
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

submit() {
    $submit_bin --properties-file $conf_file --jars $jars --class $main_class $BASE_DIR/lib/$main_jar $@ > $BASE_DIR/logs/$main_jar-$(date +'%Y%m%d-%H%M%S').out 2>&1
}

main() {
    source_env
    locate_conf $1
    mk_params
    mk_jars
    mk_main_jar
    submit $@
}

main $@
