#!/usr/bin/env bash
BIN=`cd $(dirname $0);pwd`
cd $BIN

install_archetype() {
    cd potato-assembly
    mvn clean archetype:create-from-project
    cd target/generated-sources/archetype/
    mvn install
}

create_project() {
    mvn archetype:generate
}

install_archetype
