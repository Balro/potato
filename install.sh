#!/usr/bin/env bash
BIN=$(
  cd -P "$(dirname "$0")" && pwd || exit
)

usage() {
  cat <<EOF
usage:
  install [all|proj|arch]
    # Install project or archetype to the maven repository.
  create [dir]
    # Create a new project from the quickstart archetype.
    # Arg dir specified the directory where project to create.
    # If not specified, create project on current directory.
EOF
}

install_project() {
  cd "$BIN" && mvn -DskipTests clean install
}

install_archetype() {
  cd "$BIN"/potato-quickstart && {
    mvn -DskipTests clean archetype:create-from-project
    cp bin/potato* target/generated-sources/archetype/src/main/resources/archetype-resources/bin/
    sed -i_bak 's|<include>\*\*/\*.</include>|<include>\*\*/\*</include>|g' target/generated-sources/archetype/src/main/resources/META-INF/maven/archetype-metadata.xml
    cd target/generated-sources/archetype/ && mvn -DskipTests install
  }
}

create_project() {
  if [ $# -gt 0 ]; then
    if [ -d "$1" ]; then
      cd "$1" && mvn archetype:generate \
        -DarchetypeGroupId=quickstart -DarchetypeArtifactId=potato-quickstart-archetype
    else
      echo "$1 is a not valied directory." >&2
      exit
    fi
  else
    mvn archetype:generate \
      -DarchetypeGroupId=quickstart -DarchetypeArtifactId=potato-quickstart-archetype
  fi
}

case $1 in
install)
  shift
  if [ $# -eq 0 ]; then
    install_project
    install_archetype
  fi
  case $1 in
  "all")
    install_project
    install_archetype
    ;;
  "proj" | "project")
    install_project
    ;;
  "arch" | "archetype")
    install_archetype
    ;;
  "*")
    echo "Unknown install command $1"
    ;;
  esac
  ;;
create)
  shift
  create_project "$1"
  ;;
*)
  usage
  ;;
esac
