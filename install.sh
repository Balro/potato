#!/usr/bin/env bash
BIN=`cd $(dirname $0);pwd`
cd $BIN

usage() {
    cat <<EOF
usage:
    install  # install potato and archetype to the maven repository.
    create   # create a new project from the maven archetype.
EOF
}

install() {
    cd $BIN
    mvn -DskipTests clean install
}

install_archetype() {
    cd $BIN/potato-quickstart
    mvn -DskipTests clean archetype:create-from-project
    cd target/generated-sources/archetype/
    mvn -DskipTests install
}

create_project() {
    mvn archetype:generate
}

case $1 in
    install):
        install
        install_archetype
    ;;
    create):
        create_project
    ;;
    *)
        usage
    ;;
esac
