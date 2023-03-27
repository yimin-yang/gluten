#!/bin/bash

BASEDIR=$(dirname $0)
source "$BASEDIR/builddeps-veloxbe.sh"

cd $GLUTEN_DIR
mvn clean package -Pspark-3.2 -Pbackends-velox -DskipTests -Dspark32.version=3.2.0
mvn clean package -Pbackends-velox -Pspark-3.3 -DskipTests
