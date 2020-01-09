#!/usr/bin/env bash
BASE_DIR=`cd -P $(dirname $0)/../;pwd`

usage() {
    cat << EOF
Usage:
    $(basename $0) <conf_file> <service> [service args]

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

main() {
    source_env
    locate_conf $1
    shift

    local service_shell=""
    case $1 in
        submit)
            service_shell=submit.sh
        ;;
        lock)
            service_shell=lock.sh
        ;;
        *)
            usage
            exit 1
        ;;
    esac

    source $BASE_DIR/bin/exec/$service_shell
    shift
    do_work "$@"
}

main "$@"
