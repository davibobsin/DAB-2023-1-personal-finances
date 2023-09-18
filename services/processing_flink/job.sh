#!/bin/bash
PACKAGE_NAME=processing
#PACKAGE_NAME=postgresql_sink

COMMAND=$1
PACKAGE=$2

if [[ -z $PACKAGE ]];
then
    echo "package not found"
    exit
fi

case $1 in
  build)
    cd $PACKAGE && mvn clean package
    ;;

  run)
    flink run $PACKAGE/target/$PACKAGE-0.1.jar
    ;;

  *)
    echo invalid command
    ;;
esac
